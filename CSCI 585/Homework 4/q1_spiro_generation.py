import math
import json

t=0.00
R = 8.0/10000
r = 1.0/10000
a = 4.0/10000
x0, y0 = -118.285469, 34.020546

res = []
while t < 16*math.pi:
	x = x0+(R+r)*math.cos((r/R)*t) - a*math.cos((1+r/R)*t)
	y = y0+(R+r)*math.sin((r/R)*t) - a*math.sin((1+r/R)*t)
	res.append("{\n"+f'"loc":[{x},{y}]'+"\n}")
	t+=0.1

with open("spiro_points.txt", 'w') as f:
	f.write("[\n"+",\n".join(res)+"\n]")
