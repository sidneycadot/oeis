#! /usr/bin/env python3

def digit_count(n, base):
    return 0 if n == 0 else 1 + digit_count(n // base, base)


def complement(n, base):
    return base ** digit_count(n, base) - n


class A110395:

    def __init__(self):
        self.memo = {}

    def __getitem__(self, n):

        assert n >= 1

        BASE = 10

        if n in self.memo:
            return self.memo[n]

        if n == 1:
            result = 1
        else:
            result = n * complement(self[n - 1], BASE)

        self.memo[n] = result

        return result


def main():

    a110395 = A110395()

    MIN = 1
    MAX = 200

    with open("b110395.txt", "w") as f:
        for n in range(MIN, MAX + 1):
            print(n, a110395[n], file = f)


if __name__ == "__main__":
    main()
