export interface PanelBounds {
  left: number;
  top: number;
  width: number;
  height: number;
}

export function clampPanelBounds(bounds: PanelBounds, viewportWidth: number, viewportHeight: number): PanelBounds {
  const margin = Math.min(16, viewportWidth / 4, viewportHeight / 4);
  const availableWidth = Math.max(0, viewportWidth - margin * 2);
  const availableHeight = Math.max(0, viewportHeight - margin * 2);
  const width = Math.min(availableWidth, Math.max(Math.min(280, availableWidth), bounds.width));
  const height = Math.min(availableHeight, Math.max(Math.min(180, availableHeight), bounds.height));
  return {
    width,
    height,
    left: Math.max(margin, Math.min(viewportWidth - width - margin, bounds.left)),
    top: Math.max(margin, Math.min(viewportHeight - height - margin, bounds.top)),
  };
}