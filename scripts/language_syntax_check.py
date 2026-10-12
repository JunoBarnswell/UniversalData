"""Parse binding/example C# and Java syntax without compiler or SDK execution.

Pinned prebuilt wheels: tree-sitter 0.25.2, tree-sitter-c-sharp 0.23.1,
tree-sitter-java 0.23.5 under work/static-tools. No type checking, linking,
P/Invoke/JNI layout or native lifetime validation is implied.
"""
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / 'work' / 'static-tools'))
try:
    import tree_sitter
    import tree_sitter_c_sharp
    import tree_sitter_java
except ImportError:
    print('Blocked: pinned prebuilt C#/Java tree-sitter wheels are missing')
    raise SystemExit(2)
parsers = {
    '.cs': tree_sitter.Parser(tree_sitter.Language(tree_sitter_c_sharp.language())),
    '.java': tree_sitter.Parser(tree_sitter.Language(tree_sitter_java.language())),
}
errors = []
count = 0
for folder in ('bindings', 'examples'):
    for path in sorted((root / folder).rglob('*')):
        if path.suffix not in parsers or set(path.relative_to(root).parts) & {'bin', 'obj', 'target'}:
            continue
        tree = parsers[path.suffix].parse(path.read_bytes())
        count += 1
        if not tree.root_node.has_error:
            continue
        stack = [tree.root_node]
        while stack:
            node = stack.pop()
            if node.is_missing or node.type == 'ERROR':
                errors.append(f'{path.relative_to(root)}:{node.start_point.row + 1}: syntax {node.type}')
            else:
                stack.extend(reversed(node.children))
if errors:
    print('\n'.join(errors))
    raise SystemExit(1)
print(f'Binding syntax parser passed: {count} C#/Java files; no compilation / execution performed')
