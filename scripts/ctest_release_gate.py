"""Release acceptance never turns missing resources / disabled CTest cases into success."""
import pathlib
import sys
import xml.etree.ElementTree as ET

path = pathlib.Path(sys.argv[1])
root = ET.parse(path).getroot()
cases = root.findall('.//testcase')
if not cases:
    raise SystemExit('Release blocked: no real test results')
blocked = [case.attrib.get('name', '?') for case in cases if any(case.find(kind) is not None for kind in ('failure', 'error', 'skipped'))]
if blocked:
    raise SystemExit('Release blocked: failed or unexecuted cases: ' + ', '.join(blocked))
print(f'Release CTest gate: {len(cases)} actually executed cases passed')
