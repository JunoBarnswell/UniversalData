import js from '@eslint/js';
import globals from 'globals';
import ts from 'typescript-eslint';
import hooks from 'eslint-plugin-react-hooks';
export default ts.config({ ignores: ['node_modules', '../UniversalData.Management/wwwroot'] }, js.configs.recommended, ...ts.configs.recommended, { files: ['src/**/*.{ts,tsx}'], languageOptions: { globals: globals.browser }, plugins: { 'react-hooks': hooks }, rules: { ...hooks.configs.recommended.rules } });
