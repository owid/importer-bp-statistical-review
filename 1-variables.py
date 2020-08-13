"""Given a set of metadata defined in workbook_format.csv, processes each sheet in the BP
workbook, concatenates all variables, and writes the output as variables.csv
"""

import pandas as pd


def process_sheet(sheet_name, skiprows, subvariable, unit_override):
    """Processes one sheet of the BP workbook according to the specifications of the
    workbook_format.csv file.

    Arguments:
        sheet_name {str} -- Name of the sheet in the BP workbook
        skiprows {int} -- Number of rows to skip before the row containing the units
        subvariable {str} -- If empty, the sheet is processed as normal. If filled, a variable is
        created as "subvariable". To be used to create multiple variables from one given BP sheet
        unit_override {str} -- If empty, the units are set to the name of the first column, i.e. the
        top-left cell of the sheet after applying skiprows. If filled, the value is overridden.

    Returns:
        [DataFrame] -- DataFrame with header: name,unit,notes
        Name = sheet_name if subvariable is empty, or subvariable if subvariable is filled.
        Unit = name of the first column, i.e. the top-left cell of the sheet after applying skiprows
        unless overridden by unit_override.
        Notes = extracted as the first row in the left-most column containing "Notes:|Note:"
        and all subsequent rows.

    Raises:
        Exception -- An exception is raised if after applying skiprows, the top-left cell of the
        sheet (which should be the units) is empty.
    """
    print(sheet_name)

    sheet = pd.read_excel("input/bp_data_2020.xlsx", sheet_name=sheet_name, skiprows=skiprows)

    if pd.isnull(unit_override):
        unit = sheet.columns[0]
        if unit == "Unnamed: 0":
            raise Exception(f"No unit found - skiprows is probably wrong for sheet: {sheet_name}")
    else:
        unit = unit_override

    notes_idx = sheet[sheet.iloc[:, 0].str.contains(("Notes:|Note:"), na=False)].index.values
    if len(notes_idx) > 0:
        notes_idx = notes_idx[0]
        notes = " ".join(sheet.iloc[notes_idx:, 0].values)
    else:
        notes = pd.NA

    return pd.DataFrame({
        "name": sheet_name if pd.isnull(subvariable) else subvariable,
        "unit": unit,
        "notes": [notes]
    })


def main():
    """Given a set of metadata defined in workbook_format.csv, processes each sheet in the BP
    workbook, concatenates all variables, and writes the output as variables.csv
    """

    metadata = pd.read_csv("input/workbook_format.csv")
    variables = []

    for _, row in metadata.iterrows():
        variables.append(process_sheet(
            sheet_name=row.sheet_name,
            skiprows=row.skiprows,
            subvariable=row.subvariable,
            unit_override=row.unit_override
        ))

    variables = pd.concat(variables)

    variables["id"] = range(1, len(variables)+1)
    variables = variables[["id", "name", "unit", "notes"]]
    variables.to_csv("output/variables.csv", index=False)


if __name__ == '__main__':
    main()
