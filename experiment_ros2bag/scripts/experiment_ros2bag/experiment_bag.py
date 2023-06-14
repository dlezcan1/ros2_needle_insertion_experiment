from abc import ABC, abstractmethod, abstractproperty
from collections import defaultdict
import itertools
import os
from typing import List, Dict, Tuple, Union
import warnings

import numpy as np
import pandas as pd
import sqlite3

from sensor_msgs.msg import (
    CameraInfo,
    Image,
)
from std_msgs.msg import Float64MultiArray
from geometry_msgs.msg import (
    PoseStamped,
    PoseArray,
)

from .bag_file_parser import BagFileParser
from needle_shape_publisher.utilites import (
    msg2pose,
    msg2poses,
)

from pgr_stereo_camera.utilities import (
    ImageConversions
)


class ExperimentBagParser(ABC, BagFileParser):
    def __init__(
            self,
            bagdir: str,
            insertion_depths: List[float],
            bagfile: str = None,
            yamlfile: str = None,
            topics: Union[Dict[str, List[str]], List[str]] = None,
            bag_db: sqlite3.Connection = None
        ):
        super().__init__(
            bagdir,
            bagfile=bagfile,
            yamlfile=yamlfile,
            bag_db=bag_db,
        )
        self.topics_of_interest = topics
        self.insertion_depths   = sorted(insertion_depths)

    # __init__

    @abstractmethod
    def parse_data(self):
        """ Parse the Insertion Experiment Data """
        pass

    # parse_data

    @abstractmethod
    def save_results(self, odir: str, filename: str = None):
        """ Save the results to the output directory """
        pass

    # save_results

    def get_topics_by_name(self, name: str, topic_list: List[str] = None):
        if topic_list is None:
            topic_list = self.topics_of_interest

        return list(filter(lambda topic: name in topic, topic_list))

    # get_topics_by_name

# class: ExperimentBagParser

class TimestampDependentExperimentBagParser(ExperimentBagParser, ABC):
    def __init__(
            self,
            bagdir: str,
            insertion_depths: List[float],
            bagfile: str = None,
            yamlfile: str = None,
            topics: List[str] = None,
            bag_db: sqlite3.Connection = None,
        ):
        super().__init__(bagdir, insertion_depths, bagfile, yamlfile, topics, bag_db)

        self.timestamp_ranges  = None
        self.target_timestamps = None
        self.timestamps        = dict()

    # __init__

    def configure_timestamp_ranges(self, timestamp_ranges: Dict[float, Tuple[int, int]]):
        """ Set the min and max timestamp ranges for each of the insertion depths """
        assert set(timestamp_ranges.keys()).issubset(self.insertion_depths), (
            "Configured timestamp ranges has insertion depths not setup to be processed: "
            f"{set(timestamp_ranges.keys()).difference(self.insertion_depths)}"
        )

        self.timestamp_ranges = timestamp_ranges

    # configure_timestamp_ranges

    def configure_target_timestamps(self, target_ts: Dict[float, int]):
        assert set(target_ts.keys()).issubset(self.insertion_depths), (
            "Configured target timestamps has insertion depths not setup to be processed: "
            f"{set(target_ts.keys()).difference(self.insertion_depths)}"
        )

        self.target_timestamps = target_ts

    # configure_target_timestamps

    @abstractmethod
    def determine_timestamps(self, inplace: bool = False):
        """ Method to determine which timestamps to use.
            Should return timestamps and fill self.timestamps (if inplace is True)
        """
        pass

    # determine_timestamps

    def determine_timestamps_closest_to_target(
        self,
        by_topic: str,
        target_timestamps: Dict[float, int],
        ts_range: tuple = None,
        ts_range_exclude: tuple = None,
        inplace: bool = False,
    ):
        """ Determines which timestamps to use based on a topic
            which has timestamps closest to the target timestamps
            by which topic related
        """
        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        for depth, target_ts in target_timestamps.items():
            ts_range_d = ts_range
            if ts_range_d is None and self.timestamp_ranges is not None:
                ts_range_d = self.timestamp_ranges.get(depth, None)

            closest_ts, _ = self.get_closest_message_to_timestamp(
                by_topic,
                target_ts,
                ts_range=ts_range_d,
                ts_range_exclude=ts_range_exclude,
            )

            timestamps[depth] = closest_ts

        # for

        return timestamps

    # determine_timestamps_closest_to_target

# class: TimestampDependentExperimentBagParser


class CameraDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = [
        "/camera/left/camera_info", "/camera/right/camera_info", # sensor_msgs.msg.CameraInfo
        "/camera/left/image_raw",   "/camera/right/image_raw",   # sensor_msgs.msg.Image
        "/needle/state/gt_shape",                                # geometry_msgs.msg.PoseArray
        "/needle/state/transf_camera2needle",                    # geometry_msgs.msg.Pose
        "/needle/state/gt_shape_transformed",                    # geometry_msgs.msg.PoseArray
    ]

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: List[str] = None,
        bag_db: sqlite3.Connection = None,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else CameraDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
        )

        # parsed data results
        # self.camera_data = {
        #   insertion depth: {
        #       data: {
        #           left_image  : image from left camera
        #           left_info   : camera information from left camera
        #           right_image : image from right camera
        #           right_info  : camera information from right camera
        #           needle_shape: shape of needle
        #       }
        #       timestamps: {
        #           key from data: timestamp of data topic
        #       }
        #   }
        # }
        self.camera_data = defaultdict(
            lambda: {
                "data": dict(),
                "timestamps": dict(),
            },
        )

    # __init__

    @staticmethod
    def camerainfo_msg2dict(msg: CameraInfo):
        return {
            "height"           : msg.height,
            "width"            : msg.width,
            "intrinsic"        : np.asarray(msg.k),
            "rotation"         : np.asarray(msg.r),
            "projection"       : np.asarray(msg.p),
            "distortion_model" : msg.distortion_model,
            "distortion_coeffs": np.asarray(msg.d)
        }

    # camerainfo_msg2dict

    def determine_timestamps(self, inplace: bool = False):
        assert self.target_timestamps is not None, (
            "You must configure the target timestamps for each of the insertion depths"
        )

        key_topic_1 = self.get_topics_by_name("left/gt_shpae")[0]
        key_topic_2 = self.get_topics_by_name("left/image_raw")[0]

        timestamps = self.determine_timestamps_closest_to_target(
            key_topic_1,
            self.target_timestamps,
            ts_range=self.timestamp_ranges
        )
        if any(v is None for v in timestamps.values()):
            timestamps_2 = self.determine_timestamps_closest_to_target(
                key_topic_2,
                {
                    depth: target_ts
                    for depth, target_ts in self.target_timestamps
                    if timestamps.get(depth, None) is None
                },
                ts_range=self.timestamp_ranges
            )

            # update timestamps
            timestamps.update(timestamps_2)

        # if

        if inplace:
            self.timestamps = timestamps

        return timestamps

    # determine_timestamps

    def parse_data(self):
        assert self.timestamps is not None, (
            "You must determine which timestamps to use for each of the insertions depths in order to "
            "parse the camera data"
        )
        # topic names
        gtshape_topic   = self.get_topics_by_name("needle/state/gt_shape")[0]

        leftimg_topic   = self.get_topics_by_name("left/image_raw")[0]
        rightimg_topic  = self.get_topics_by_name("righ/image_raw")[0]

        leftinfo_topic  = self.get_topics_by_name("left/camera_info")[0]
        rightinfo_topic = self.get_topics_by_name("righ/camera_info")[0]

        for depth, target_ts in self.timestamps.items():
            ts_range = self.timestamp_ranges.get(depth)

            # get data by gt_shape (if possible)
            closest_gtshape_ts, best_gtshape_msg = self.get_closest_message_to_timestamp(
                gtshape_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_gtshape = None
            if best_gtshape_msg is not None:
                best_gtshape_msg: PoseArray
                best_gtshape = msg2poses(best_gtshape_msg)[:, :3, 3]
            
            # if
            self.camera_data[depth]["data"]["gt_shape"]       = best_gtshape
            self.camera_data[depth]["timestamps"]["gt_shape"] = closest_gtshape_ts
            
            if best_gtshape is not None:
                continue # no need to process images
            
            # get data by raw images (back-up)
            # - left image
            closest_limg_ts, best_limg_msg = self.get_closest_message_to_timestamp(
                leftimg_topic,
                target_ts,
                ts_range=ts_range,
            )
            closest_linfo_ts, best_linfo_msg = self.get_closest_message_to_timestamp(
                leftinfo_topic,
                closest_limg_ts,
                ts_range=ts_range,
            )

            best_limg = None
            if best_limg_msg is not None:
                best_limg_msg: Image
                best_limg = ImageConversions.ImageMsgTocv(best_limg_msg)

            self.camera_data[depth]["data"]["left_image"]      = best_limg
            self.camera_data[depth]["timestamp"]["left_image"] = closest_limg_ts

            # if
            best_linfo = None
            if best_linfo_msg is not None:
                best_linfo_msg: CameraInfo
                best_linfo = self.camerainfo_msg2dict(best_linfo_msg)

            # if

            self.camera_data[depth]["data"]["left_info"]      = best_linfo
            self.camera_data[depth]["timestamp"]["left_info"] = closest_linfo_ts
            
            # - right image
            closest_rimg_ts, best_rimg_msg = self.get_closest_message_to_timestamp(
                rightimg_topic,
                target_ts,
                ts_range=ts_range,
            )
            closest_rinfo_ts, best_rinfo_msg = self.get_closest_message_to_timestamp(
                rightinfo_topic,
                closest_rimg_ts,
                ts_range=ts_range,
            )

            best_rimg = None
            if best_rimg_msg is not None:
                best_rimg_msg: Image
                best_rimg = ImageConversions.ImageMsgTocv(best_rimg_msg)

            self.camera_data[depth]["data"]["right_image"]      = best_rimg
            self.camera_data[depth]["timestamp"]["right_image"] = closest_rimg_ts

            # if
            best_rinfo = None
            if best_rinfo_msg is not None:
                best_rinfo_msg: CameraInfo
                best_rinfo = self.camerainfo_msg2dict(best_rinfo_msg)

            # if

            self.camera_data[depth]["data"]["right_info"]      = best_rinfo
            self.camera_data[depth]["timestamp"]["right_info"] = closest_rinfo_ts

        # for

        return self.camera_data

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        # TODO
        pass

    # save_results

# class: CameraDataBagParser

class RobotDataBagParser(ExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = ["/stage/state/needle_pose"] + [
        "/".join([ns, topic, axis])
        for ns, topic, axis in
        itertools.product(
            ["/stage/axis"],
            ["command", "position", "state/moving", "state/on"],
            ["linear_stage", "x", "y", "z"]
        )
    ]

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: List[str] = None,
        bag_db: sqlite3.Connection = None,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
        )

        self.key_topic = self.get_topics_by_name("state/needle_pose")[0]

        # data members (from parsing the bag)
        self.insertion_depth_timestamp_ranges = None

    # __init__

    def parse_data(self):
        bag_rows = self.get_messages(
            topic_name=self.key_topic,
            generator_count=1,
        )

        insertion_depth_timestamp_ranges = defaultdict(list)

        for i, (ts, topic, msg) in enumerate(bag_rows):
            insertion_depth = self.get_insertion_depth(msg)

            for d in self.insertion_depths:
                if abs(d - insertion_depth) < 1e-6:
                    insertion_depth_timestamp_ranges[d].append(ts)
                    break
                # if
            # for

        # for

        self.insertion_depth_timestamp_ranges = {
            d: ( min(timestamps), max(timestamps) )
            for d, timestamps in insertion_depth_timestamp_ranges.items()
        }

        return self.insertion_depth_timestamp_ranges

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        assert self.insertion_depth_timestamp_ranges is not None, (
            "Robot data has not been parsed yet."
        )
        filename = filename if filename is not None else "robot_timestamp_ranges.csv"

        ts_ranges_df = pd.DataFrame.from_dict(
            self.insertion_depth_timestamp_ranges,
            orient='index',
            columns=['ts_min', 'ts_max'],
        )

        os.makedirs(odir, exist_ok=True)
        ts_ranges_df.to_csv(
            os.path.join(odir, filename),
            header=True,
            index=True,
            index_label="Insertion Depth (mm)"
        )
        print(
            "Saved robot timestamp data to:",
            os.path.join(odir, filename)
        )

    # save_results

    @classmethod
    def get_insertion_depth(cls, needle_pose_msg: PoseStamped):
        """ Returns the insertion depth of the state/needle_pose topic message """
        return needle_pose_msg.pose.position.z

    # get_insertion_depth

# class: RobotDataBagParser

class NeedleDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = [
        "/needle/sensor/raw",
        "/needle/sensor/processed",
        "/needle/state/current_shape",
        "/needle/state/kappac",
        "/needle/state/winit",
        "/needle/state/skin_entry",
        "/stage/state/needle_pose",
    ]

    def __init__(
            self,
            bagdir: str,
            insertion_depths: List[float],
            bagfile: str = None,
            yamlfile: str = None,
            topics: List[str] = None,
            bag_db: sqlite3.Connection = None,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else NeedleDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
        )

        # parsed data results
        # self.needle_data = {
        #   insertion depth: {
        #       data: {
        #           kappa_c: (N, )  floats,
        #           winit:   (N, )  floats,
        #           shape:   (N, 3) floats,
        #       }
        #       timestamps: {
        #           key from data: timestamp of data topic
        #       }
        #   }
        # }
        self.needle_data = defaultdict(
            lambda: {
                "data": dict(),
                "timestamps": dict(),
            },
        )

    # __init__

    def determine_timestamps(self, inplace: bool=False):
        assert self.timestamp_ranges is not None, (
            "You must configure the timestamp ranges for each of the insertion depths in order to "
            "determine the timestamps"
        )

        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        kappac_topic = self.get_topics_by_name("state/kappac")[0]
        for depth, ts_range in self.timestamp_ranges.items():
            kappac_msgs = self.get_messages(kappac_topic, ts_range=ts_range, generator_count=-1)

            viable_ts_msg = list()
            for ts, _, kc_msg in kappac_msgs:
                kc_msg: Float64MultiArray

                if all(kc == 0 for kc in kc_msg.data): # try to skip all zero kappa c
                    continue

                viable_ts_msg.append((ts, kc_msg))

            # for

            best_ts = kappac_msgs[len(kappac_msgs)//2][0]
            if len(viable_ts_msg) > 0:
                best_ts = viable_ts_msg[len(viable_ts_msg)//2][0]

            else:
                warnings.warn(f"Insertion Depth {depth} mm has kappa_c = 0 for all messages.")

            timestamps[depth] = best_ts

        # for

        return timestamps

    # determine_timestamps

    def parse_data(self):
        assert self.timestamps is not None, (
            "You must determine which timestamps to use for each of the insertions depths in order to "
            "parse the needle shape data"
        )

        # topic names
        kappac_topic = self.get_topics_by_name("state/kappac")
        winit_topic  = self.get_topics_by_name("state/winit")
        shape_topic  = self.get_topics_by_name("state/current_shape")

        # parse the needle shape data
        for depth, ts_range in self.timestamp_ranges.items():
            target_ts = self.timestamps[depth]

            closest_kc_ts, best_kc_msg = self.get_closest_message_to_timestamp(
                kappac_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_kc = None
            if best_kc_msg is not None:
                best_kc_msg: Float64MultiArray
                best_kc = np.asarray(best_kc_msg.data)

            # if

            self.needle_data[depth]["data"]["kappa_c"]      = best_kc
            self.needle_data[depth]["timestamp"]["kappa_c"] = closest_kc_ts

            # parse the needle's winit
            closest_winit_ts, best_winit_msg = self.get_closest_message_to_timestamp(
                winit_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_winit = None
            if best_winit_msg is not None:
                best_winit_msg: Float64MultiArray
                best_winit = np.asarray(best_winit_msg.data)

            # if

            self.needle_data[depth]["data"]["winit"]       = best_winit
            self.needle_data[depth]["timestamps"]["winit"] = closest_winit_ts

            # parse the needle's shape
            closest_shape_ts, best_shape_msg = self.get_closest_message_to_timestamp(
                shape_topic,
                target_ts,
                ts_range=ts_range
            )
            best_shape = None
            if best_shape_msg is not None:
                best_shape_msg: PoseArray
                best_shape = msg2poses(best_shape)[:, :3, 3] # take only the positions

            # if

            self.needle_data[depth]["data"]["shape"]       = best_shape
            self.needle_data[depth]["timestamps"]["shape"] = closest_shape_ts

        # for

        return self.needle_data

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        assert len(self.needle_data) > 0, (
            "Needle data needs to be parsed first!"
        )

        filename = filename if filename is not None else "needle_data.xlsx"

        for depth, data_ts_dict in self.needle_data.items():
            sub_odir = os.path.join(odir, str(depth))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename)) as xl_writer:
                # write the timestamp information (timestamps grabbed)
                ts_df = pd.DataFrame.from_dict(data_ts_dict["timestamps"], orient='index')
                ts_df.loc["target"]    = self.timestamps[depth]
                ts_df.loc["range_min"] = self.timestamp_ranges[depth][0]
                ts_df.loc["range_max"] = self.timestamp_ranges[depth][1]

                ts_df.to_excel(
                    xl_writer,
                    sheet_name="ROS timestamps",
                    index=True,
                )

                # write the data values
                for data_name, data in data_ts_dict["data"].items():
                    df = pd.DataFrame(data)
                    df.to_excel(
                        xl_writer,
                        sheet_name=data_name,
                        index=False,
                    )

                # for
            # with

            print(
                f"Saved needle shape data for insertion depth {depth} mm to:",
                os.path.join(sub_odir, filename)
            )

        # for

    # save_results

# class: NeedleDataBagParser

class InsertionExperimentBagParser( ExperimentBagParser ):
    DEFAULT_TOPICS_OF_INTEREST = {
        "camera": CameraDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "robot" : RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "needle": NeedleDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
    }

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: Dict[str, List[str]] = None,
        bag_db: sqlite3.Connection = None,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else InsertionExperimentBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
        )

        self.needle_parser = NeedleDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["needle"],
            bag_db=self.bag_db,
        )
        self.robot_parser = RobotDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["robot"],
            bag_db=self.bag_db,
        )
        self.camera_parser = CameraDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["camera"],
            bag_db=self.bag_db,
        )

        self._to_parse = {
            "robot":  False,
            "camera": False,
            "needle": False,
        }

    # __init__

    def configure(
        self,
        parse_robot:  bool = False,
        parse_camera: bool = False,
        parse_needle: bool = False,
    ):
        self._to_parse["robot"]  = parse_robot
        self._to_parse["camera"] = parse_camera
        self._to_parse["needle"] = parse_needle

    # configure

    def parse_data(self):
        robot_timestamp_ranges = {
            depth: (None, None)
            for depth in self.insertion_depths
        }
        if self._to_parse["robot"]:
            robot_timestamp_ranges = self.robot_parser.parse_data()

        # if

        needle_data       = None
        needle_timestamps = None
        if self._to_parse["needle"]:
            self.needle_parser.configure_timestamp_ranges(robot_timestamp_ranges)

            needle_timestamps = self.needle_parser.determine_timestamps(inplace=True)

            needle_data = self.needle_parser.parse_data()

        # if

        camera_data       = None
        camera_timestamps = None
        if self._to_parse["camera"]:
            self.camera_parser.configure_timestamp_ranges(robot_timestamp_ranges)
            self.camera_parser.configure_target_timestamps(needle_timestamps)

            camera_timestamps = self.camera_parser.determine_timestamps()

            camera_data = self.camera_parser.parse_data()

        # if

        return (robot_timestamp_ranges, needle_data, camera_data)

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        # TODO
        pass

    # save_results

# class: InsertionExperimentBag