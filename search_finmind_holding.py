from FinMind.data import DataLoader
dl = DataLoader()
methods = [m for m in dir(dl) if not m.startswith('_')]
matching = [m for m in methods if 'holding' in m.lower()]
print("Matching methods:", matching)
