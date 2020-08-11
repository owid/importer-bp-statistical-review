from glob import glob
import pandas as pd

def main():

    countries = set()

    for filename in glob("output/datapoints/*.csv"):
        tmp = pd.read_csv(filename)
        countries |= set(tmp["country"])

    res = pd.DataFrame({"name": list(countries)}).sort_values("name")

    res.to_csv("standardization/entities.csv", index=False)

if __name__ == "__main__":
    main()
