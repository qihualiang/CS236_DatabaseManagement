import matplotlib.pyplot as plt
import matplotlib.patches as patches
import sys

mbrs_file = sys.argv[1]
points_file = sys.argv[2]
 
fig = plt.figure(1, figsize=(50, 50), dpi=90)
with open(mbrs_file) as f:
    ax = fig.add_subplot(111)
    lines = f.readlines()
    for line in lines:
        data = line.split(",")
        x = float(data[0])
        y = float(data[2])
        W = float(data[1]) - float(data[0])
        H = float(data[3]) - float(data[2]) 
        rect = patches.Rectangle((x, y), W, H, linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)

with open(points_file) as f:
    x = []
    y = []
    lines = f.readlines()
    for line in lines:
        data = line.split(",")
        x.append(float(data[0]))
        y.append(float(data[1]))
    plt.scatter(x, y)


plt.savefig("plot.png")