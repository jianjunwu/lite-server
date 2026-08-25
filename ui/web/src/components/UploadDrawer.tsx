import { useEffect, useState } from 'react';
import { App, AutoComplete, Button, Checkbox, Drawer, Form, Input, Progress, Typography, Upload } from 'antd';
import { InboxOutlined } from '@ant-design/icons';
import type { UploadFile } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { uploadModelFilesResumable } from '../api/upload';
import type { UploadHandle } from '../api/mutations';
import { useTasks } from '../context/TaskContext';
import { formatBytes } from './format';
import { MONO_FONT, TYPE } from '../theme';

interface UploadDrawerProps {
  open: boolean;
  onClose: () => void;
  existingModels: string[];
  /** Preselected model name (e.g. from a model detail page). */
  model?: string;
}

/**
 * Upload model files (.lma single artifact or raw files) as ONE multipart
 * request — the server finalizes the version atomically per request, so
 * per-file splitting is not an option; progress is overall, not per-file.
 */
export function UploadDrawer({ open, onClose, existingModels, model }: UploadDrawerProps) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const queryClient = useQueryClient();
  const { addTask, updateTask } = useTasks();
  const [form] = Form.useForm<{ model: string; version: string; load: boolean }>();
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [uploading, setUploading] = useState<{ percent: number; handle: UploadHandle } | null>(null);

  // Warn before leaving mid-upload.
  useEffect(() => {
    if (!uploading) return;
    const guard = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener('beforeunload', guard);
    return () => window.removeEventListener('beforeunload', guard);
  }, [uploading]);

  const totalSize = fileList.reduce((sum, f) => sum + (f.size ?? 0), 0);
  const lmaCount = fileList.filter((f) => f.name.endsWith('.lma')).length;
  const lmaConflict = lmaCount > 0 && fileList.length > 1;

  const submit = async (values: { model: string; version: string; load: boolean }) => {
    if (!instanceId) return;
    const files = fileList.map((f) => f.originFileObj as File).filter(Boolean);
    if (files.length === 0) {
      message.error(t('upload.noFiles'));
      return;
    }
    const taskId = addTask({
      title: t('upload.taskTitle', { model: values.model, version: values.version }),
      kind: 'upload',
      progress: 0,
    });
    const handle = uploadModelFilesResumable(instanceId, values.model, values.version, files, {
      load: values.load,
      onProgress: (percent, loaded, total) => {
        setUploading((cur) => (cur ? { ...cur, percent } : cur));
        updateTask(taskId, { progress: percent, detail: `${formatBytes(loaded)} / ${formatBytes(total)}` });
      },
    });
    setUploading({ percent: 0, handle });
    try {
      const result = await handle.promise;
      if (result.loaded) {
        updateTask(taskId, { status: 'success', progress: 100, detail: t('upload.loadedOk') });
      } else if (result.load_error) {
        updateTask(taskId, { status: 'error', detail: result.load_error });
      } else {
        updateTask(taskId, { status: 'success', progress: 100 });
      }
      message.success(t('upload.success', { model: result.model, version: result.version }));
      await queryClient.invalidateQueries({ queryKey: [instanceId] });
      setFileList([]);
      form.resetFields();
      onClose();
    } catch (err) {
      const text = err instanceof Error ? err.message : String(err);
      updateTask(taskId, { status: 'error', detail: text });
      message.error(text);
    } finally {
      setUploading(null);
    }
  };

  return (
    <Drawer
      title={t('upload.title')}
      open={open}
      onClose={() => {
        if (!uploading) onClose();
      }}
      width={420}
      maskClosable={!uploading}
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ model: model ?? '', version: '', load: true }}
        onFinish={submit}
        disabled={uploading !== null}
      >
        <Form.Item name="model" label={t('upload.model')} rules={[{ required: true }]}>
          <AutoComplete
            options={existingModels.map((m) => ({ value: m }))}
            placeholder={t('upload.modelPlaceholder')}
            style={{ fontFamily: MONO_FONT }}
          />
        </Form.Item>
        <Form.Item name="version" label={t('upload.version')} rules={[{ required: true }]}>
          <Input placeholder="v1" style={{ fontFamily: MONO_FONT }} />
        </Form.Item>
        <Form.Item label={t('upload.files')} required>
          <Upload.Dragger
            multiple
            fileList={fileList}
            beforeUpload={() => false}
            onChange={({ fileList: next }) => setFileList(next)}
            disabled={uploading !== null}
          >
            <p className="ant-upload-drag-icon"><InboxOutlined /></p>
            <p className="ant-upload-text">{t('upload.dragger')}</p>
            <p className="ant-upload-hint">{t('upload.draggerHint')}</p>
          </Upload.Dragger>
          {lmaConflict && (
            <Typography.Text type="danger" style={{ fontSize: TYPE.secondary }}>
              {t('upload.lmaConflict')}
            </Typography.Text>
          )}
          {fileList.length > 0 && (
            <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>
              {fileList.length} {t('upload.filesSelected')} · {formatBytes(totalSize)}
            </Typography.Text>
          )}
        </Form.Item>
        <Form.Item name="load" valuePropName="checked">
          <Checkbox>{t('upload.loadAfter')}</Checkbox>
        </Form.Item>
        {uploading && <Progress percent={uploading.percent} size="small" />}
        <Form.Item style={{ marginBottom: 0, marginTop: 16 }}>
          <Button
            type="primary"
            htmlType="submit"
            block
            disabled={fileList.length === 0 || lmaConflict}
            loading={uploading !== null}
          >
            {uploading ? t('upload.uploading') : t('upload.submit')}
          </Button>
          {uploading && (
            <Button block danger style={{ marginTop: 8 }} onClick={() => uploading.handle.abort()}>
              {t('upload.cancel')}
            </Button>
          )}
        </Form.Item>
      </Form>
    </Drawer>
  );
}
