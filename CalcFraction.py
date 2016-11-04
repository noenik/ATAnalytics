delays = open('airportToNumdelay.txt', 'r')
deps = open('output/Reducer-final.output.txt', 'r')

dels = []
max_dels = {'aid': 0, 'dels': 0, 'totaldeps': 0}
min_dels = {'aid': 0, 'dels': 100, 'totaldeps': 0}

for del_line in delays:
    iddel = del_line.split(':')
    ap = iddel[0].strip()
    total_dels = float(iddel[1].strip())
    dels.append((ap,total_dels))

for line in deps:
    iddep = line.split(':')
    airport = iddep[0].strip()
    total_deps = float(iddep[1].strip())

    for ap, total_dels in dels:
        if airport == ap:
            fract = total_dels/total_deps
            fract *= 100

            if fract > max_dels['dels']:
                max_dels['aid'] = ap
                max_dels['dels'] = fract
                max_dels['totaldeps'] = total_deps
            elif fract < min_dels['dels']:
                min_dels['aid'] = ap
                min_dels['dels'] = fract
                min_dels['totaldeps'] = total_deps

            print('Airport %s had %1.2f%% delays' % (ap, fract))

print('Most efficient airport had %1.2f%% delays from %s departures' % (min_dels['dels'], min_dels['totaldeps']))
print('Least efficient airport had %1.2f%% delays from %s departures' % (max_dels['dels'], max_dels['totaldeps']))

delays.close()
deps.close()
