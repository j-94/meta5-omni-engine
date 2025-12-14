import itertools, json, math, random
from pathlib import Path

def dist(p, q):
    return math.hypot(p[0]-q[0], p[1]-q[1])

cities = [ (random.uniform(0, 100), random.uniform(0, 100)) for _ in range(10) ]

best_path, best_d = None, float('inf')
for perm in itertools.permutations(range(1, len(cities))):
    path = (0, *perm)
    d = sum(dist(cities[path[i]], cities[path[i+1]]) for i in range(len(path)-1))
    d += dist(cities[path[-1]], cities[path[0]])  # return to start
    if d < best_d:
        best_path, best_d = path, d

result = {
    "cities": cities,
    "tour": list(best_path),
    "distance": best_d
}
print(f"Best tour: {result['tour']}\nDistance: {result['distance']:.3f}")

Path("logs").mkdir(exist_ok=True)
with open("logs/tsp_solution.json", "w") as f:
    json.dump(result, f, indent=2)