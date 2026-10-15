import pathlib, subprocess, sys, tempfile
with tempfile.TemporaryDirectory(prefix='ud-designer-native-') as directory:
    root=pathlib.Path(directory)
    csv=root/'source.csv'
    csv.write_text('Amount,CustomerType,Region\n'+''.join(f'{i},{"VIP" if i%2 else "Normal"},North\n' for i in range(10)),encoding='utf-8')
    subprocess.run([sys.argv[1],str(csv),str(root/'source.xlsx')],check=True)
