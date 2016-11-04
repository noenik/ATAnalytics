M1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
M2 = [[1, 2], [3, 4], [5, 6], [7, 8]]
Res_M = []

print('M1:')
for row in M1:
    print(row)

print('M2:')
for row in M2:
    print(row)

# M1 * M2: M1s height and M2s width

dim_M1 = [len(M1[0]), len(M1)]
dim_M2 = [len(M2[0]), len(M2)]

for i in range(0, dim_M1[1]):
    Res_M.append([])
    for j in range(0, dim_M2[0]):
        M1_ij = M1[i][j]

        for row in M2:
            val = row[i]
        Res_M[i].append([])
        Res_M[i][j] = [M1[i][j], M2[i][j]]

# print("Dim M1:")
# print(dim_M1)
# print("Dim M2:")
# print(dim_M2)

print('')
for row in Res_M:
    print(row)
