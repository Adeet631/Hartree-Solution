import pandas as pd
import os
import logging

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

group_by_cols_list = ["legal_entity", "counter_party", "tier"]


def read_dataset_files():
    """
    Read dataset files and return DataFrames.

    Returns:
        DataFrame, DataFrame: Two DataFrames from dataset files.
    """
    file1_path = os.path.join("data", "dataset1.csv")
    file2_path = os.path.join("data", "dataset2.csv")
    df1 = pd.read_csv(file1_path, sep=",")
    df2 = pd.read_csv(file2_path, sep=",")
    return df1, df2


def read_and_merge_dataframes():
    """
    Merge dataframes and return the merged DataFrame.

    Returns:
        DataFrame: Merged DataFrame.
    """
    logger.info("Reading and merging datasets")
    df1, df2 = read_dataset_files()
    df_merged = pd.merge(df1, df2, on="counter_party", how="outer")
    logger.info("Dataframes merged successfully")
    return df_merged


def generate_dataframe(merged_df, column_group):
    """
    Create DataFrames based on the provided columns.

    Parameters:
    merged_df: The merged dataframe.
    column_group: List of columns to group the DataFrame by.

    Returns:
    DataFrame: A new DataFrame with grouping based on specified columns.
    """
    grouped_df = (
        merged_df.groupby(column_group)
        .agg(
            max_rating=("rating", "max"),
            ARAP=("value", lambda x: x[merged_df["status"] == "ARAP"].sum()),
            ACCR=("value", lambda x: x[merged_df["status"] == "ACCR"].sum()),
        )
        .reset_index()
    )
    for column in group_by_cols_list:
        if column not in column_group:
            grouped_df[column] = "Total"

    return grouped_df


def create_output_file(final_df):
    """
    Saves the final DataFrame to a CSV file.

    Parameters:
    final_df : The DataFrame to be saved to a CSV file.

    Returns:
    None: This function does not return any value.
    """
    final_result_file = "pandas_output.csv"
    final_df.to_csv(final_result_file, index=False)
    logger.info("Pandas File Generated Successfully")


def generate_pandas_output_file():
    """
    Read and merge CSV files, generate grouped DataFrames based on different column pairs,
    and save the combined results to a CSV file in the 'output' directory.

    Parameters:
    None

    Returns:
    None: This function does not return any value.
    """
    try:
        # Reading and merging CSV files
        merged_df = read_and_merge_dataframes()

        final_df = pd.DataFrame({})

        possible_column_pairs = [
            ["legal_entity"],
            ["counter_party"],
            ["tier"],
            ["legal_entity", "counter_party"],
        ]

        # Create DataFrames for all column groups and return a combined result DataFrame.
        for groupby_columns in possible_column_pairs:
            grouped_df = generate_dataframe(merged_df, groupby_columns)
            final_df = pd.concat([final_df, grouped_df], ignore_index=True)

        final_df = final_df[
            [
                "legal_entity",
                "counter_party",
                "tier",
                "max_rating",
                "ARAP",
                "ACCR",
            ]
        ]

        # Save results to output file
        create_output_file(final_df)

    except Exception as e:
        logger.info(e)
        logger.info("Pandas File failed to be created")


if __name__ == "__main__":
    logger.info("Data aggregation process started")
    generate_pandas_output_file()
