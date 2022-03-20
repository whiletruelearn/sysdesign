import string
import random
import pandas as pd


def main():
    items = []
    for i in range(1, 101):
        key = "".join([random.choice(string.ascii_letters)
                       for _ in range(0, random.randint(3, 10))])
        value = "".join([random.choice(string.ascii_letters)
                         for _ in range(0, random.randint(3, 10))])
        items.append([key, value])

    pd.DataFrame(items, columns=["key", "value"]).to_csv(
        "test_bed.csv", index=False)


if __name__ == "__main__":
    main()
