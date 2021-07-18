
q1 = {}
z = 'a'
for i in range(12):
    if z not in q1:
        q1[z] = []
    q1[z].append(i)
    if len(q1[z]) > 7:
        q1[z].pop(0)

print(q1)