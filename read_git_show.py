import subprocess
result = subprocess.run(['git', 'show', '0d3ff40db468c70a:frontend/src/pages/Dashboard.jsx'], capture_output=True, text=True, encoding='utf-8')
print(result.stdout[:1000])

