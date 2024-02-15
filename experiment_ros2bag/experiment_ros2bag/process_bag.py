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

    insertion_trial_group = parser.add_mutually_exclusive_group(
        required=True,
    )
    insertion_trial_group.add_argument(
        "--insertion-depths",
        nargs="+",
        type=float,
        default=list(),
        help="The insertion depths used in this experiment",
    )
    insertion_trial_group.add_argument(
        "--use-unique-robot-poses",
        action="store_true",
        help="Use the the unique robot poses as an experiment trial key"
    )

    parser.add_argument(
        "--odir",
        type=str,
        default=None,
        required=True,
        help="The output directory to save the results to."
    )

    parser.add_argument(
        "--show-plots",
        action="store_true"
    )

    for data_type, topics in InsertionExperimentBagParser.DEFAULT_TOPICS_OF_INTEREST.items():
        parser.add_argument(
            f"--parse-{data_type}",
            action="store_true",
        )

        parser.add_argument(
            f"--topics-{data_type}",
            nargs="+",
            default=topics,
            help=f"The topics of interest for {data_type} parsing",
            metavar=f"topics_{data_type.replace('-', '_')}",
        )

    parser.add_argument("--debug", action="store_true")

    return parser

# __get_argparser

def main( args=None ):
    parser = __get_argparser()

    ARGS        = parser.parse_args( args )
    ARGS_KWARGS = dict(ARGS._get_kwargs())

    if ARGS.debug:
        PORT = 4444
        debugpy.listen(PORT)
        print(f"Debugger listening on port {PORT}. Waiting for client...")
        debugpy.wait_for_client()
        print("Debugger connected.")
        print("\n" + 80*"=" + "\n")

    # if: debugger

    topics_of_interest = {
        data_type: ARGS_KWARGS.get(f"topics_{data_type.replace('-', '_')}")
        for data_type in InsertionExperimentBagParser.DEFAULT_TOPICS_OF_INTEREST.keys()
    }

    bag = InsertionExperimentBagParser(
        bagdir=ARGS.bagdir,
        insertion_depths=ARGS.insertion_depths,
        bagfile=ARGS.bag_file,
        yamlfile=ARGS.yaml_file,
        topics=topics_of_interest,
        use_insertion_depths_only=not ARGS.use_unique_robot_poses,
    )

    bag.configure(
        parse_robot           = ARGS.parse_robot,
        parse_needle          = ARGS.parse_needle,
        parse_fbg             = ARGS.parse_fbg,
        parse_camera          = ARGS.parse_camera,
        parse_insertion_point = ARGS.parse_insertion_point,
    )

    bag.parse_data()

    bag.plot_results(
        odir=ARGS.odir if not ARGS.show_plots else None,
        show=ARGS.show_plots,
    )
    
    bag.save_results(
        odir=ARGS.odir,
    )

# main

if __name__ == "__main__":
    main()

# if __main__