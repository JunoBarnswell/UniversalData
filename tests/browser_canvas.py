"""Browser gestures wait for React Flow's layout and hit targets to settle."""
from playwright.sync_api import expect


def connect(page, source, target):
    page.get_by_role('button', name='Fit View', exact=True).click()
    count = page.locator('.react-flow__edge').count()
    output = page.locator(f'.react-flow__node[data-id="{source}"] .react-flow__handle.source').first
    input_ = page.locator(f'.react-flow__node[data-id="{target}"] .react-flow__handle.target').first
    output.drag_to(input_, timeout=10000)
    expect(page.locator('.react-flow__edge')).to_have_count(count + 1)
