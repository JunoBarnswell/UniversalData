import type { ComponentProps } from "react";

export function Button({ type = "button", ...props }: ComponentProps<"button">) {
  return <button type={type} {...props} />;
}

export function Input(props: ComponentProps<"input">) {
  return <input {...props} />;
}

export function Select(props: ComponentProps<"select">) {
  return <select {...props} />;
}

export function TextArea(props: ComponentProps<"textarea">) {
  return <textarea {...props} />;
}
