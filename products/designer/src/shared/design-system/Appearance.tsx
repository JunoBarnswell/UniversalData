import { useEffect, useState } from "react";
export function Appearance() {
  const [theme, setTheme] = useState(
      () => localStorage.getItem("ud.studio.theme") ?? "system",
    ),
    [density, setDensity] = useState(
      () => localStorage.getItem("ud.studio.density") ?? "comfortable",
    );
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("ud.studio.theme", theme);
  }, [theme]);
  useEffect(() => {
    document.documentElement.dataset.density = density;
    localStorage.setItem("ud.studio.density", density);
  }, [density]);
  return (
    <div className="appearance-controls">
      <select
        aria-label="主题"
        value={theme}
        onChange={(event) => setTheme(event.target.value)}
      >
        <option value="system">跟随系统</option>
        <option value="light">浅色</option>
        <option value="dark">深色</option>
      </select>
      <select
        aria-label="界面密度"
        value={density}
        onChange={(event) => setDensity(event.target.value)}
      >
        <option value="comfortable">舒适</option>
        <option value="compact">紧凑</option>
      </select>
    </div>
  );
}
