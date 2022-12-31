#! /usr/bin/env python3

class A137419:

    def __init__(self):
        self.memo = {
            1: 1,
            2: 2,
            3: 2,
            4: 1
        }

    def __getitem__(self, n):

        assert n >= 1

        if n in self.memo:
            return self.memo[n]

        result = self[n - 4] + 1 - ((-1) ** self[self[n - 1]] + 1) * (self[self[n - 1]] - self[self[n - 2]]) // 2

        #(self[n - 4] + 1 - ((-1) ** self[self[n - 1]] + 1) * (a(a(n - 1)) - a(a(n - 2)))/2)
        
        self.memo[n] = result

        return result


def main():

    a137419 = A137419()

    MIN = 1
    MAX = 132  # higher values are undefined.

    with open("b137419.txt", "w") as f:
        for n in range(MIN, MAX + 1):
            print(n, a137419[n], file = f)

if __name__ == "__main__":
    main()
