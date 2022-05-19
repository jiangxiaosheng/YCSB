import sys
import matplotlib.pyplot as plt

cpu_num = 8

file_name = sys.argv[1]
agg_num = int(sys.argv[2])
figure_name = file_name[:-4]
data = []

agg_cnt = 0
with open(file_name, 'r') as f:
    line = f.readline()
    
    while line:
        if line.startswith('2'):
            nums = line.split()
            if nums[6] != 'est':
                if agg_cnt % agg_num == 0:
                    if len(data) > 0:
                        data[-1] /= agg_num
                    data.append(0)

                agg_cnt += 1

                data[-1] += float(nums[6])

        line = f.readline()

    if agg_cnt % agg_num != 0:
        if len(data) > 0:
            data[-1] /= agg_num

time = [i * agg_num for i in range(len(data))]

plt.figure()
plt.plot(time, data, label='YCSB')
plt.xlabel('Second')
plt.ylabel('Throughput (op/s)')
plt.ylim([0, 80000])
plt.legend()
plt.savefig(figure_name + '.jpg')
plt.close()
