import argparse

import debugpy

from .experiment_ros2bag.experiment_bag import InsertionExperimentBagParser


def __get_argparser():
    """ Configure argument parser """
    parser = argparse.ArgumentParser()

    parser.add_argument( 
        "bagdir", 
        type=str,
        help="The bag directory to use"    
    )

    parser.add_argument(
        "--bag-file",
        type=str,
        default=None,
        required=False,
    )

    parser.add_argument(
        "--yaml-file",
        type=str,
        default=None,
        required=False,
    )

    parser.add_argument(
        "--insertion-depths",
        nargs="+",
        type=float,
        default=list(),
        required=True,
        help="The insertion depths used in this experiment",
    )

    parser.add_argument(
        "--odir",
        type=str,
        default=None,
        required=True,
        help="The output directory to save the results to."
    )

    for data_type in [
        "robot", 
        "needle", 
        "fbg", 
        "camera"
    ]:
        parser.add_argument(
            f"--parse-{data_type}",
            action="store_true"
        )

    parser.add_argument("--debug", action="store_true")


    return parser


# __get_argparser

def main( args=None ):
    parser = __get_argparser()

    ARGS = parser.parse_args( args )

    if ARGS.debug:
        PORT = 4444
        debugpy.listen(PORT)
        print(f"Debugger listening on port {PORT}. Waiting for client...")
        debugpy.wait_for_client()
        print("Debugger connected.")
        print("\n" + 80*"=" + "\n")

    # if: debugger

    bag = InsertionExperimentBagParser(
        bagdir=ARGS.bagdir,
        insertion_depths=ARGS.insertion_depths,
        bagfile=ARGS.bag_file,
        yamlfile=ARGS.yaml_file,
    )

    bag.configure(
        parse_robot  = ARGS.parse_robot,
        parse_needle = ARGS.parse_needle,
        parse_fbg    = ARGS.parse_fbg,
        parse_camera = ARGS.parse_camera,
    )

    bag.parse_data()

    bag.save_results(
        odir=ARGS.odir,
    )


# main

if __name__ == "__main__":
    main()

# if __main__