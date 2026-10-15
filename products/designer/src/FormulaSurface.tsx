import { useEffect, useImperativeHandle, useRef, type Ref } from "react";
import { Annotation, EditorState } from "@codemirror/state";
import {
  EditorView,
  keymap,
  lineNumbers,
  highlightActiveLine,
  highlightSpecialChars,
} from "@codemirror/view";
import {
  StreamLanguage,
  bracketMatching,
  indentOnInput,
  syntaxHighlighting,
  defaultHighlightStyle,
} from "@codemirror/language";
import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
  type CompletionContext,
} from "@codemirror/autocomplete";
import {
  defaultKeymap,
  history,
  historyKeymap,
  indentWithTab,
} from "@codemirror/commands";
import {
  linter,
  lintGutter,
  forceLinting,
  type Diagnostic,
} from "@codemirror/lint";
import type { Component, DataField } from "./model";
export interface NativeCompilation {
  result: number;
  start: number;
  length: number;
  message: string;
  schema?: import("./model").DataSchema;
}
export interface FormulaHandle {
  insert: (value: string) => void;
  select: (from: number, to: number) => void;
}
const synchronized = Annotation.define<boolean>();
const language = StreamLanguage.define({
  token(stream) {
    if (stream.eatSpace()) return null;
    if (stream.match(/^"(?:\\.|[^"\\])*"?/)) return "string";
    if (stream.match(/^@\[(?:[^\]]|\]\])*\]/)) return "variableName";
    if (stream.match(/^\[(?:[^\]]|\]\])*\]/)) return "propertyName";
    if (stream.match(/^\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[ULF]*/i)) return "number";
    if (stream.match(/^DT_[A-Z0-9]+/i)) return "typeName";
    if (stream.match(/^[A-Za-z_][\w.]*(?=\s*\()/))
      return "function(variableName)";
    if (stream.match(/^[A-Za-z_][\w.]*/)) return "variableName";
    stream.next();
    return "operator";
  },
});
export function utf8Position(source: string, offset: number) {
  return new TextDecoder().decode(
    new TextEncoder().encode(source).subarray(0, offset),
  ).length;
}
export function FormulaSurface({
  value,
  change,
  fields,
  functions,
  variables,
  compile,
  editorRef,
}: {
  value: string;
  change: (value: string) => void;
  fields: DataField[];
  functions: Component[];
  variables: string[];
  compile?: (source: string) => Promise<NativeCompilation>;
  editorRef: Ref<FormulaHandle>;
}) {
  const container = useRef<HTMLDivElement>(null),
    editor = useRef<EditorView | null>(null);
  const settings = useRef({ change, fields, functions, variables, compile });
  useEffect(() => {
    settings.current = { change, fields, functions, variables, compile };
    if (editor.current) forceLinting(editor.current);
  }, [change, fields, functions, variables, compile]);
  useImperativeHandle(
    editorRef,
    () => ({
      insert(text) {
        const view = editor.current;
        if (!view) return;
        const range = view.state.selection.main;
        view.dispatch({
          changes: { from: range.from, to: range.to, insert: text },
          selection: { anchor: range.from + text.length },
        });
        view.focus();
      },
      select(from, to) {
        const view = editor.current;
        if (!view) return;
        view.dispatch({
          selection: { anchor: from, head: to },
          scrollIntoView: true,
        });
        view.focus();
      },
    }),
    [],
  );
  useEffect(() => {
    if (!container.current) return;
    let live = true;
    function completions(context: CompletionContext) {
      const word = context.matchBefore(/[\w\u0080-\uFFFF@.[\]]*/);
      if (!word || (!context.explicit && word.from === word.to)) return null;
      const data = settings.current;
      return {
        from: word.from,
        options: [
          ...data.fields.map((field) => ({
            label: field.name,
            type: "property",
            detail: field.scalar?.type ?? field.type,
            apply: `[${field.name.replaceAll("]", "]]")}]`,
          })),
          ...data.functions.map((fn) => ({
            label: fn.id.toUpperCase(),
            type: "function",
            detail: `${fn.minimumArguments ?? 0}–${fn.maximumArguments ?? 0} 参数`,
            info: fn.name,
            apply: fn.id.toUpperCase() + "()",
          })),
          ...data.variables.map((name) => ({
            label: "@[" + name + "]",
            type: "variable",
          })),
        ],
      };
    }
    const view = new EditorView({
      parent: container.current,
      state: EditorState.create({
        doc: "",
        extensions: [
          lineNumbers(),
          highlightSpecialChars(),
          history(),
          highlightActiveLine(),
          bracketMatching(),
          indentOnInput(),
          closeBrackets(),
          language,
          syntaxHighlighting(defaultHighlightStyle),
          autocompletion({ override: [completions] }),
          keymap.of([
            ...closeBracketsKeymap,
            ...defaultKeymap,
            ...historyKeymap,
            ...completionKeymap,
            indentWithTab,
          ]),
          lintGutter(),
          linter(
            async (current) => {
              const source = current.state.doc.toString(),
                operation = settings.current.compile;
              if (!operation || !source) return [];
              try {
                const result = await operation(source);
                if (!live || !result.result) return [];
                const from = Math.min(
                    source.length,
                    utf8Position(source, result.start),
                  ),
                  to = Math.min(
                    source.length,
                    Math.max(
                      from + 1,
                      utf8Position(source, result.start + result.length),
                    ),
                  );
                return [
                  { from, to, severity: "error", message: result.message },
                ] as Diagnostic[];
              } catch {
                return [];
              }
            },
            { delay: 550 },
          ),
          EditorView.lineWrapping,
          EditorView.contentAttributes.of({
            "aria-label": "SSIS 公式",
            spellcheck: "false",
          }),
          EditorView.updateListener.of((update) => {
            if (
              update.docChanged &&
              !update.transactions.some((transaction) =>
                transaction.annotation(synchronized),
              )
            )
              settings.current.change(update.state.doc.toString());
          }),
        ],
      }),
    });
    editor.current = view;
    return () => {
      live = false;
      editor.current = null;
      view.destroy();
    };
  }, []);
  useEffect(() => {
    const view = editor.current;
    if (view && view.state.doc.toString() !== value)
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value },
        annotations: synchronized.of(true),
      });
  }, [value]);
  return <div className="formula-surface" ref={container} />;
}
