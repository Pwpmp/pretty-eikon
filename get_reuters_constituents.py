import os
_PATH_CLEAN = '../../ra_data/reuters/clean'
comps = os.listdir(_PATH_CLEAN)

def get_name(filename):
    filename = filename.split('_')
    if filename[1].isdigit():
        return filename[0]
    else:
        return ".".join(filename[:2])

comps = set([get_name(x) for x in comps])
with open('reuters_constituents.csv', 'w') as f:
    f.write('comp\n')
    for comp in comps:
        f.write('%s\n' % comp)