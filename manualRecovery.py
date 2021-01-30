with open('../dissertation/NCAR-referencesManual.csv', 'r', encoding='utf-8') as f:
	lines = f.readlines()

with open('../dissertation/NCAR-referencesManual-recovery.csv', 'w', encoding='utf-8') as f:
	f.write(lines.pop(0))
	f.write(lines.pop(0))
	for line in lines:
		line = line.strip()
		if line[0] != ',':
			f.write(line.strip(',') + ',\n')
		else:
			line = line.strip(',')
			line = ''.join([s.strip() for s in line.split('"')])
			f.write(',"{}"\n'.format(line))

