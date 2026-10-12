"""Parse C17 syntax without invoking a compiler or running SDK code.

Requires the pinned tree-sitter wheels in work/static-tools. This verifies syntax,
not type checking, linking, ABI layout, transactions, or runtime correctness.
"""
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / 'work' / 'static-tools'))
try:
    import tree_sitter
    import tree_sitter_c
except ImportError:
    print('Blocked: install pinned tree-sitter==0.25.2 / tree-sitter-c==0.24.1 wheels into work/static-tools')
    raise SystemExit(2)
parser = tree_sitter.Parser(tree_sitter.Language(tree_sitter_c.language()))
failed = []
count = 0
for folder in ('components', 'engines', 'hosts', 'plugins', 'include', 'tests', 'examples/c', 'bindings/java/native'):
    for path in sorted((root / folder).rglob('*')):
        if path.suffix not in {'.c', '.h'}:
            continue
        source = path.read_text(encoding='utf-8')
        source = re.sub(r'#ifdef __cplusplus\s+extern "C" \{\s+#endif', '', source)
        source = re.sub(r'#ifdef __cplusplus\s+\}\s+#endif', '', source)
        source = re.sub(r'\b(?:JNIEXPORT|JNICALL)\b', '', source)
        source = re.sub(r'\bUD_(?:API|PLUGIN_EXPORT)\b(?=\s+(?:ud_status|const|void|uint\w*|int\w*|size_t))', '', source)
        source = '\n'.join(line if line.lstrip().startswith('#') else line.replace('UD_SCHEMA_HEADER', '"ud-schema 2\\n"').replace('UD_PLAN_HEADER', '"ud-plan 2\\n"') for line in source.split('\n'))
        tree = parser.parse(source.encode('utf-8'))
        count += 1
        if not tree.root_node.has_error:
            continue
        stack = [tree.root_node]
        while stack:
            node = stack.pop()
            if node.type == 'ERROR' or node.is_missing:
                failed.append(f'{path.relative_to(root)}:{node.start_point.row + 1}: C syntax {node.type}')
            else:
                stack.extend(reversed(node.children))
if failed:
    print('\n'.join(failed))
    raise SystemExit(1)
print(f'C syntax parser passed: {count} files; no compilation / execution performed')
