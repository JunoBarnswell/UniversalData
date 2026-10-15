export function ResizeHandle({
  label,
  orientation = "vertical",
  value,
  min,
  max,
  change,
  reverse = false,
}: {
  label: string;
  orientation?: "horizontal" | "vertical";
  value: number;
  min: number;
  max: number;
  change: (value: number) => void;
  reverse?: boolean;
}) {
  const bound = (next: number) => change(Math.max(min, Math.min(max, next)));
  return (
    <div
      className={`resize-handle ${orientation}`}
      role="separator"
      tabIndex={0}
      aria-label={label}
      aria-orientation={orientation}
      aria-valuemin={min}
      aria-valuemax={max}
      aria-valuenow={value}
      onKeyDown={(event) => {
        if (
          ["ArrowLeft", "ArrowUp", "ArrowRight", "ArrowDown"].includes(
            event.key,
          )
        ) {
          event.preventDefault();
          bound(
            value +
              (["ArrowLeft", "ArrowUp"].includes(event.key) ? -20 : 20) *
                (reverse ? -1 : 1),
          );
        }
      }}
      onPointerDown={(event) => {
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        const start =
          orientation === "vertical" ? event.clientX : event.clientY;
        const initial = value;
        event.currentTarget.onpointermove = (next) =>
          bound(
            initial +
              ((orientation === "vertical" ? next.clientX : next.clientY) -
                start) *
                (reverse ? -1 : 1),
          );
        event.currentTarget.onpointerup = () => {
          event.currentTarget.onpointermove = null;
          event.currentTarget.onpointerup = null;
        };
      }}
    />
  );
}
