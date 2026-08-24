import { Badge, Button, Drawer, Empty, Progress, Typography } from 'antd';
import { BellOutlined, CheckCircleFilled, CloseCircleFilled, DeleteOutlined } from '@ant-design/icons';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTasks, type UiTask } from '../context/TaskContext';
import { STATUS_COLORS, TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

function TaskRow({ task, onDismiss }: { task: UiTask; onDismiss: (id: string) => void }) {
  const neutrals = useNeutrals();
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: `1px solid ${neutrals.border}` }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: TYPE.body, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {task.title}
        </div>
        {task.status === 'running' && (
          <Progress percent={task.progress} size="small" style={{ marginBottom: 0 }} />
        )}
        {task.detail && (
          <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>
            {task.detail}
          </Typography.Text>
        )}
      </div>
      {task.status === 'success' && <CheckCircleFilled style={{ color: STATUS_COLORS.ready }} />}
      {task.status === 'error' && <CloseCircleFilled style={{ color: STATUS_COLORS.error }} />}
      {task.status !== 'running' && (
        <Button type="text" size="small" icon={<DeleteOutlined />} onClick={() => onDismiss(task.id)} />
      )}
    </div>
  );
}

/** Header bell + drawer: async tasks (uploads, loads) live here so the user
 * can navigate away after submitting. */
export function TaskBell() {
  const { t } = useTranslation();
  const { tasks, dismissTask, clearFinished } = useTasks();
  const [open, setOpen] = useState(false);
  const running = tasks.filter((t) => t.status === 'running').length;

  return (
    <>
      <Badge count={running} size="small">
        <Button type="text" icon={<BellOutlined />} onClick={() => setOpen(true)} aria-label="tasks" />
      </Badge>
      <Drawer
        title={t('tasks.title')}
        open={open}
        onClose={() => setOpen(false)}
        width={360}
        extra={
          tasks.some((t) => t.status !== 'running') && (
            <Button size="small" onClick={clearFinished}>
              {t('tasks.clearFinished')}
            </Button>
          )
        }
      >
        {tasks.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={t('tasks.empty')} />
        ) : (
          tasks.map((task) => <TaskRow key={task.id} task={task} onDismiss={dismissTask} />)
        )}
      </Drawer>
    </>
  );
}
