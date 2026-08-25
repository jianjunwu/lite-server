import { ApartmentOutlined, CodeOutlined, DeploymentUnitOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { SERIES_COLORS } from '../theme';

const TYPE_ICONS: Record<string, typeof CodeOutlined> = {
  litapi: CodeOutlined,
  ensemble: ApartmentOutlined,
};

/** djb2 — deterministic tint per model name, stable across reloads. */
function nameHash(name: string): number {
  let h = 5381;
  for (let i = 0; i < name.length; i++) h = ((h << 5) + h + name.charCodeAt(i)) >>> 0;
  return h;
}

interface ModelGlyphProps {
  name: string;
  type?: string;
  /** Square plate edge in px (list cards 32, detail header 48). */
  size?: number;
}

/**
 * Model faceplate: the glyph encodes the model type, the plate tint derives
 * deterministically from the model name — every model is recognizable at a
 * glance with zero configuration.
 */
export function ModelGlyph({ name, type = 'unknown', size = 32 }: ModelGlyphProps) {
  const { t } = useTranslation();
  const Icon = TYPE_ICONS[type.toLowerCase()] ?? DeploymentUnitOutlined;
  const tint = SERIES_COLORS[nameHash(name) % SERIES_COLORS.length];
  return (
    <span
      role="img"
      aria-label={t('models.glyphLabel', { type })}
      className="glyph"
      style={{ width: size, height: size, background: `${tint}1F`, color: tint }}
    >
      <Icon aria-hidden style={{ fontSize: Math.round(size * 0.52) }} />
    </span>
  );
}
