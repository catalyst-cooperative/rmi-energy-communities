"""A skeleton of a command line interface to be deployed as an entry point script.

It takes two numbers and does something to them, printing out the result.

"""

from __future__ import annotations

import argparse
import logging
import sys

import energy_comms

# This is the module-level logger, for any logs
logger = logging.getLogger(__name__)


def parse_command_line(argv: list[str]) -> argparse.Namespace:
    """Parse command line arguments. See the -h option for details.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """

    def formatter(prog) -> argparse.HelpFormatter:  # type: ignore
        """This is a hack to create HelpFormatter with a particular width."""
        return argparse.HelpFormatter(prog, width=88)

    # Use the module-level docstring as the script's description in the help message.
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=formatter)

    parser.add_argument(
        "-ca",
        "--coal_area",
        type=str,
        help="The type of qualifying area for the coal criteria. Options are 'tract', 'county', 'state'.",
        default="tract",
    )
    parser.add_argument(
        "-ba",
        "--brownfields_area",
        type=str,
        help="The type of qualifying area for the brownfields criteria. Options are 'tract', 'county', 'state'.",
        default="tract",
    )
    parser.add_argument(
        "-u",
        "--update_employment_data",
        type=bool,
        help="Get a fresh download of data for the employment criteria.",
        default=False,
    )
    parser.add_argument(
        "-f",
        "--output_filepath",
        type=str,
        help="Absolute or relative file path to save the pickled output dataframe.",
        default="output_df.pkl",
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def main() -> int:
    """Get dataframe of all qualifying areas and save pickled dataframe."""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
        level=logging.INFO,
    )

    args = parse_command_line(sys.argv)
    print("Creating dataframe of all qualifying energy communities.")
    df = energy_comms.coordinate.get_all_qualifying_areas(
        coal_census_geometry=args.coal_area,
        brownfields_census_geometry=args.brownfields_area,
        update_employment=args.update_employment_data,
    )
    df.to_pickle(args.output_filepath)
    return 0


def coal_criteria() -> None:
    """Get dataframe of coal criteria areas and save pickled dataframe."""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
        level=logging.INFO,
    )
    args = parse_command_line(sys.argv)
    print("Creating dataframe of coal criteria qualifying energy communities.")
    df = energy_comms.coordinate.get_coal_criteria_qualifying_areas(
        census_geometry=args.coal_area
    )
    df.to_pickle(args.output_filepath)


def brownfields_criteria() -> None:
    """Get dataframe of brownfields criteria areas and save pickled dataframe."""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
        level=logging.INFO,
    )
    args = parse_command_line(sys.argv)
    print("Creating dataframe of brownfields criteria qualifying energy communities.")
    df = energy_comms.coordinate.get_brownfields_criteria_qualifying_areas(
        census_geometry=args.brownfields_area
    )
    df.to_pickle(args.output_filepath)


def employment_criteria() -> None:
    """Get dataframe of employment criteria areas and save pickled dataframe."""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
        level=logging.INFO,
    )
    args = parse_command_line(sys.argv)
    print("Creating dataframe of employment criteria qualifying energy communities.")
    df = energy_comms.coordinate.get_employment_criteria_qualifying_areas(
        update=args.update_employment_data
    )
    df.to_pickle(args.output_filepath)


if __name__ == "__main__":
    sys.exit(main())
