from abc import ABC, abstractmethod, abstractproperty
from collections import defaultdict
import itertools
import os
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Union,
)
import warnings

from rclpy.time import (
    Duration,
    Time,
)
import numpy as np
import pandas as pd
import sqlite3
import cv2 as cv
import matplotlib.pyplot as plt

from sensor_msgs.msg import (
    CameraInfo,
    Image,
)
from std_msgs.msg import Float64MultiArray
from geometry_msgs.msg import (
    Pose,
    PoseStamped,
    PoseArray,
    Point,
)

import needle_shape_sensing as nss
from .bag_file_parser import BagFileParser
import needle_shape_publisher.utilities as nsp_util

import pgr_stereo_camera.utilities as pgr_util

from needle_insertion_robot_translation.nodes import CoordinateConversions as StageCoordinateConversions


class ExperimentBagParser(ABC, BagFileParser):
    def __init__(
            self,
            bagdir: str,
            insertion_depths: List[float],
            bagfile: str = None,
            yamlfile: str = None,
            topics: Union[Dict[str, List[str]], List[str]] = None,
            bag_db: sqlite3.Connection = None,
            use_insertion_depths_only: bool = True
        ):
        super().__init__(
            bagdir,
            bagfile=bagfile,
            yamlfile=yamlfile,
            bag_db=bag_db,
        )
        self.topics_of_interest    = topics
        self.use_insertion_depths  = use_insertion_depths_only
        self.insertion_depths      = sorted(insertion_depths)
        self.experiment_trial_keys = list()

    # __init__
        
    @property
    def trial_keys(self):
        if self.use_insertion_depths:
            self.experiment_trial_keys = self.insertion_depths
        
        return self.experiment_trial_keys
    
    # property: trial_keys

    @abstractmethod
    def parse_data(self):
        """ Parse the Insertion Experiment Data """
        pass

    # parse_data

    def is_parsed(self):
        """ Return if the dataset is parsed or not """
        return False

    # is_parsed

    @abstractmethod
    def save_results(self, odir: str, filename: str = None):
        """ Save the results to the output directory """
        pass

    # save_results

    def configure_trial_keys(self, trial_keys):
        if self.use_insertion_depths:
            assert all(map(lambda v: isinstance(v, float), trial_keys)), (
                "These must be insertion depths"
            )
            self.insertion_depths = self.trial_keys

        else:
            self.experiment_trial_keys = trial_keys

    # configure_trial_keys

    def get_topics_by_name(self, name: str, topic_list: List[str] = None):
        if topic_list is None:
            topic_list = self.topics_of_interest

        return list(filter(lambda topic: name in topic, topic_list))

    # get_topics_by_name

    def plot_results(self, odir: str = None, show: bool = False):
        """ Plot the results of the work """
        pass

    # plot_results

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
            use_insertion_depths_only: bool = True
        ):
        super().__init__(bagdir, insertion_depths, bagfile, yamlfile, topics, bag_db, use_insertion_depths_only)

        self.timestamp_ranges  = None
        self.target_timestamps = None
        self.timestamps        = dict()

    # __init__

    def configure_timestamp_ranges(self, timestamp_ranges: Dict[Any, Tuple[int, int]]):
        """ Set the min and max timestamp ranges for each of the trial keys """
        assert set(timestamp_ranges.keys()).issubset(self.trial_keys), (
            "Configured timestamp ranges has experiment trial keys not setup to be processed: "
            f"{set(timestamp_ranges.keys()).difference(self.trial_keys)}"
        )

        self.timestamp_ranges = timestamp_ranges

    # configure_timestamp_ranges

    def configure_target_timestamps(self, target_ts: Dict[Any, int]):
        assert set(target_ts.keys()).issubset(self.trial_keys), (
            "Configured target timestamps has experiment trial keys not setup to be processed: "
            f"{set(target_ts.keys()).difference(self.trial_keys)}"
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
        target_timestamps: Dict[Any, int],
        ts_range: Union[tuple, Dict[float, Tuple[int, int]]] = None,
        ts_range_exclude: Union[tuple, Dict[float, Tuple[int, int]]] = None,
        inplace: bool = False,
    ):
        """ Determines which timestamps to use based on a topic
            which has timestamps closest to the target timestamps
            by which topic related
        """
        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        for trial_key, target_ts in target_timestamps.items():
            ts_range_d = ts_range
            if (
                ((ts_range_d is None)
                and (self.timestamp_ranges is not None))
                or isinstance(ts_range_d, dict)
            ):
                ts_range_d = self.timestamp_ranges.get(trial_key, None)

            # elif

            closest_ts, _ = self.get_closest_message_to_timestamp(
                by_topic,
                target_ts,
                ts_range=ts_range_d,
                ts_range_exclude=ts_range_exclude,
            )

            timestamps[trial_key] = closest_ts

        # for

        return timestamps

    # determine_timestamps_closest_to_target

    @staticmethod
    def determine_median_timestamp(timestamps: Union[List, np.ndarray]):
        """ Determine which timestamp is the median one"""
        timestamps = np.asarray(timestamps)

        median_ts = np.median(timestamps)

        if median_ts in timestamps:
            return median_ts

        closest_ts = timestamps[
            np.argmin(np.abs(timestamps - median_ts))
        ]

        return closest_ts

    # determine_median_timestamp

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
        use_insertion_depths_only: bool = True,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else CameraDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
            use_insertion_depths_only=use_insertion_depths_only
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

    def is_parsed(self):
        return len(self.camera_data) > 0

    # is_parsed

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
        assert (
            self.target_timestamps is not None
            or self.timestamp_ranges is not None
        ), (
            "You must configure the target timestamps for each of the insertion depths and/or the timestamp ranges"
        )
        timestamps = dict()
        if self.target_timestamps is not None:
            key_topic_1 = self.get_topics_by_name("state/gt_shape")[0]
            key_topic_2 = self.get_topics_by_name("left/image_raw")[0]

            timestamps = self.determine_timestamps_closest_to_target(
                key_topic_1,
                self.target_timestamps,
                ts_range=self.timestamp_ranges,
            )

            if any(v is None for v in timestamps.values()):
                timestamps_2 = self.determine_timestamps_closest_to_target(
                    key_topic_2,
                    {
                        depth: target_ts
                        for depth, target_ts in self.target_timestamps.items()
                        if timestamps.get(depth, None) is None
                    },
                    ts_range=self.timestamp_ranges
                )

                # update timestamps
                timestamps.update(timestamps_2)

            # if
        # if: target timestamps addedbag_rows =
        else:
            gtshape_topic = self.get_topics_by_name("state/gt_shape")[0]
            leftimg_topic = self.get_topics_by_name("left/image_raw")[0]
            for trial_key, ts_range in self.timestamp_ranges.items():
                gtshape_msgs = self.get_all_messages(gtshape_topic, ts_range=ts_range)

                ts_depth = np.asarray([
                    ts for ts, *_ in gtshape_msgs
                ])

                if len(ts_depth) == 0:
                    warnings.warn(f"Camera data did not find any messages for {gtshape_topic} when determining timestamps!")
                    leftimg_msgs = self.get_all_messages(leftimg_topic, ts_range=ts_range)
                    ts_depth = np.asarray([
                        ts for ts, *_ in leftimg_msgs
                    ])

                # if

                if len(ts_depth) == 0: # skip this one
                    warnings.warn(f"Camera data did not have any timestamp information for insertion depth {trial_key} mm")
                    continue

                timestamps[trial_key] = self.determine_median_timestamp(ts_depth)

            # for

        # else


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
        rightimg_topic  = self.get_topics_by_name("right/image_raw")[0]

        leftinfo_topic  = self.get_topics_by_name("left/camera_info")[0]
        rightinfo_topic = self.get_topics_by_name("right/camera_info")[0]

        for trial_key, target_ts in self.timestamps.items():
            ts_range = self.timestamp_ranges.get(trial_key)

            # get data by gt_shape (if possible)
            closest_gtshape_ts, best_gtshape_msg = self.get_closest_message_to_timestamp(
                gtshape_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_gtshape = None
            if best_gtshape_msg is not None:
                best_gtshape_msg: PoseArray
                best_gtshape = nsp_util.msg2poses(best_gtshape_msg)[:, :3, 3]

            # if
            self.camera_data[trial_key]["data"]["gt_shape"]       = best_gtshape
            self.camera_data[trial_key]["timestamps"]["gt_shape"] = closest_gtshape_ts

            if best_gtshape is not None:
                continue # no need to process images

            # get data by raw images (back-up)
            # - left image
            closest_limg_ts, best_limg_msg = self.get_closest_message_to_timestamp(
                leftimg_topic,
                target_ts,
                ts_range=ts_range,
            )
            closest_linfo_ts, best_linfo_msg = None, None
            if closest_limg_ts is not None:
                closest_linfo_ts, best_linfo_msg = self.get_closest_message_to_timestamp(
                    leftinfo_topic,
                    closest_limg_ts,
                    ts_range=ts_range,
                )

            # if

            best_limg = None
            if best_limg_msg is not None:
                best_limg_msg: Image
                best_limg = pgr_util.ImageConversions.ImageMsgTocv(best_limg_msg)

            self.camera_data[trial_key]["data"]["left_image"]       = best_limg
            self.camera_data[trial_key]["timestamps"]["left_image"] = closest_limg_ts

            # if
            best_linfo = None
            if best_linfo_msg is not None:
                best_linfo_msg: CameraInfo
                best_linfo = self.camerainfo_msg2dict(best_linfo_msg)

            # if

            self.camera_data[trial_key]["data"]["left_info"]       = best_linfo
            self.camera_data[trial_key]["timestamps"]["left_info"] = closest_linfo_ts

            # - right image
            closest_rimg_ts, best_rimg_msg = self.get_closest_message_to_timestamp(
                rightimg_topic,
                target_ts,
                ts_range=ts_range,
            )
            closest_rinfo_ts, best_rinfo_msg = None, None
            if closest_rimg_ts is not None:
                closest_rinfo_ts, best_rinfo_msg = self.get_closest_message_to_timestamp(
                    rightinfo_topic,
                    closest_rimg_ts,
                    ts_range=ts_range,
                )

            # if

            best_rimg = None
            if best_rimg_msg is not None:
                best_rimg_msg: Image
                best_rimg = pgr_util.ImageConversions.ImageMsgTocv(best_rimg_msg)

            self.camera_data[trial_key]["data"]["right_image"]       = best_rimg
            self.camera_data[trial_key]["timestamps"]["right_image"] = closest_rimg_ts

            # if
            best_rinfo = None
            if best_rinfo_msg is not None:
                best_rinfo_msg: CameraInfo
                best_rinfo = self.camerainfo_msg2dict(best_rinfo_msg)

            # if

            self.camera_data[trial_key]["data"]["right_info"]       = best_rinfo
            self.camera_data[trial_key]["timestamps"]["right_info"] = closest_rinfo_ts

        # for

        return self.camera_data

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        filename = filename if filename is not None else "camera_data.xlsx"

        for trial_key, data_ts_dict in self.camera_data.items():
            sub_odir = os.path.join(odir, str(trial_key))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename), engine='xlsxwriter') as xl_writer:
                # write the timestamp information (timestamps grabbed)
                ts_df = pd.DataFrame.from_dict(data_ts_dict["timestamps"], orient='index')
                ts_df.loc["target"]    = self.timestamps[trial_key]
                ts_df.loc["range_min"] = self.timestamp_ranges[trial_key][0]
                ts_df.loc["range_max"] = self.timestamp_ranges[trial_key][1]

                ts_df.to_excel(
                    xl_writer,
                    sheet_name="ROS timestamps",
                    index=True,
                    header=False,
                )

                # write the gt shapes
                gt_shape = data_ts_dict["data"].get("gt_shape", None)
                if gt_shape is not None:
                    df = pd.DataFrame(gt_shape)
                    df.to_excel(
                        xl_writer,
                        sheet_name="gt_shape",
                        index=False,
                        header=False,
                    )

                # if

                # write the images
                left_img = data_ts_dict["data"].get("left_image", None)
                if left_img is not None:
                    cv.imwrite(
                        os.path.join(sub_odir, "left.png"),
                        cv.cvtColor(left_img, cv.COLOR_RGB2BGR),
                    )
                    print("Saved image to:", os.path.join(sub_odir, "left.png"))

                # if

                left_info = data_ts_dict["data"].get("left_info", None)
                if left_info is not None:
                    info_df = pd.DataFrame.from_dict(left_info, orient='index')
                    info_df.to_excel(
                        xl_writer,
                        sheet_name="left_camera_info",
                        index=True,
                        header=False,
                    )

                # if

                right_img = data_ts_dict["data"].get("right_image", None)
                if right_img is not None:
                    cv.imwrite(
                        os.path.join(sub_odir, "right.png"),
                        cv.cvtColor(right_img, cv.COLOR_RGB2BGR),
                    )
                    print("Saved image to:", os.path.join(sub_odir, "right.png"))

                # if

                right_info = data_ts_dict["data"].get("right_info", None)
                if right_info is not None:
                    info_df = pd.DataFrame.from_dict(right_info, orient='index')
                    info_df.to_excel(
                        xl_writer,
                        sheet_name="right_camera_info",
                        index=True,
                        header=False,
                    )

                # if
            # with

            print(
                f"Saved camera data for trial key {trial_key} to:",
                os.path.join(sub_odir, filename)
            )

        # for

        # handle video writing
        self.write_video(os.path.join(odir, "insertion_experiment_video.avi"))

    # save_results

    def write_video(
            self,
            outfile: str,
            fps: float = 30,
            fourcc: str = "XVID",
        ):
        """ Writes the entire stereo video stream to file"""
        video_writer: cv.VideoWriter = None

        limg_topic = self.get_topics_by_name("left/image_raw")[0]
        rimg_topic = self.get_topics_by_name("right/image_raw")[0]

        # limg_ts_topic_msg
        ts_topics_msg = self.get_messages(
            [limg_topic, rimg_topic],
            generator_count=1
        )

        limg = rimg = None
        saved_first_image_pair = False
        for ts, topic, msg in ts_topics_msg:
            if topic == limg_topic:
                limg = cv.cvtColor(
                    pgr_util.ImageConversions.ImageMsgTocv(msg),
                    cv.COLOR_RGB2BGR
                )
            # if

            elif topic == rimg_topic:
                rimg = cv.cvtColor(
                    pgr_util.ImageConversions.ImageMsgTocv(msg),
                    cv.COLOR_RGB2BGR
                )
            # elif

            if (limg is None) or (rimg is None):
                continue

            # save each of the initial images
            if not saved_first_image_pair:
                odir = os.path.split(outfile)[0]
                cv.imwrite(os.path.join(odir, "initial_left.png"), limg)
                print(
                    "Saved intial left image to:",
                    os.path.join(odir, "initial_left.png")
                )

                cv.imwrite(os.path.join(odir, "initial_right.png"), rimg)
                print(
                    "Saved intial right image to:",
                    os.path.join(odir, "initial_right.png")
                )

                saved_first_image_pair = True

            # if

            # initalize video writer
            lr_img = np.concatenate((limg, rimg), axis=1)
            if video_writer is None:
                video_writer = cv.VideoWriter(
                    outfile,
                    cv.VideoWriter.fourcc(*fourcc),
                    fps,
                    (lr_img.shape[1], lr_img.shape[0]),
                    isColor=True,
                )

            # if

            # write the frame
            video_writer.write(lr_img)

        # for

        if video_writer is not None:
            video_writer.release()
            print(f"Recorded experiment video to: {outfile}")

        # if

    # write_video


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
        use_insertion_depths_only: bool = True
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
            use_insertion_depths_only=use_insertion_depths_only
        )

        self.key_topic = self.get_topics_by_name("state/needle_pose")[0]

        # data members (from parsing the bag)
        self.insertion_depth_timestamp_ranges               = None
        self.insertion_depth_poses: Dict[float, np.ndarray] = dict()

        # dictionary of directory name and pose
        self.unique_robot_poses: Dict[str, np.ndarray] = list() 
        self.unique_robot_poses_timestamp_ranges  = None

    # __init__

    def identify_unique_robot_poses(self, time_in_pose_seconds: float = 3.0, pose_tolerance: float = 1e-6):
        bag_rows = self.get_messages(
            topic_name=self.key_topic,
            generator_count=1,
        )

        robot_pose_time_lo_hi: List[Tuple[PoseStamped, float, float]] = list()
        for i, (ts, topic, msg) in enumerate(bag_rows):
            msg: PoseStamped

            # check if we switched and we need to remove a pose
            if len(robot_pose_time_lo_hi) == 0:
                robot_pose_time_lo_hi.append((msg, ts, ts))
                continue
            # if

            prev_pose_msg, prev_ts_lo, prev_ts_hi = robot_pose_time_lo_hi.pop()
            prev_pose = prev_pose_msg.pose

            # check if previous is the same
            if (
                (abs(prev_pose.position.x - msg.pose.position.x) <= pose_tolerance)
                and (abs(prev_pose.position.y - msg.pose.position.y) <= pose_tolerance)
                and (abs(prev_pose.position.z - msg.pose.position.z) <= pose_tolerance)
                and (abs(prev_pose.orientation.x - msg.pose.orientation.x) <= pose_tolerance)
                and (abs(prev_pose.orientation.y - msg.pose.orientation.y) <= pose_tolerance)
                and (abs(prev_pose.orientation.z - msg.pose.orientation.z) <= pose_tolerance)
                and (abs(prev_pose.orientation.w - msg.pose.orientation.w) <= pose_tolerance)
            ):  # last pose is same

                # update the longest time
                robot_pose_time_lo_hi.append((msg, prev_ts_lo, ts))

            # if
            else:
                # validate if last pose is within proper timestamps
                if (prev_ts_hi - prev_ts_lo) >= time_in_pose_seconds*1e9: # not long enough. remove
                    robot_pose_time_lo_hi.insert(-1, (prev_pose_msg, prev_ts_lo, prev_ts_hi))

                robot_pose_time_lo_hi.append((msg, ts, ts))

            # else

        # for

        # post-process the pose timestamp ranges
        robot_pose_timestamp_ranges : Dict[PoseStamped, Tuple[float, float]] =  {
            msg: (ts_lo, ts_hi)
            for msg, ts_lo, ts_hi in robot_pose_time_lo_hi
        }

        return robot_pose_timestamp_ranges

    # identify_unique_robot_poses

    def is_parsed(self):
        parsed = False
        if (
            self.use_insertion_depths
            and (self.insertion_depth_timestamp_ranges is not None)
        ):
            parsed = True

        elif (
            not self.use_insertion_depths
            and (self.unique_robot_poses_timestamp_ranges is not None)
        ):
            parsed = True

        return parsed

    # is_parsed

    @property
    def trial_key_pose_dict(self):
        if self.use_insertion_depths:
            return self.insertion_depth_poses
        
        return self.unique_robot_poses
    
    # directory_pose_dict

    def parse_data(self):
        if self.use_insertion_depths:
            return self.parse_data_insertion_depths()
        
        return self.parse_data_unique_poses()

    def parse_data_insertion_depths(self):
        bag_rows = self.get_messages(
            topic_name=self.key_topic,
            generator_count=1,
        )

        insertion_depth_timestamp_ranges = defaultdict(list)

        for i, (ts, topic, msg) in enumerate(bag_rows):
            msg: PoseStamped
            insertion_depth = self.get_insertion_depth(msg)

            for target_depth in self.insertion_depths:
                if abs(target_depth - insertion_depth) < 1e-6:
                    tf = np.eye(4)
                    tf[:3, 3], tf[:3, :3] = nsp_util.msg2pose(msg.pose)
                    self.insertion_depth_poses[target_depth] = tf
                    insertion_depth_timestamp_ranges[target_depth].append(ts)
                    break
                # if
            # for

        # for

        self.insertion_depth_timestamp_ranges = {
            d: ( min(timestamps), max(timestamps) )
            for d, timestamps in insertion_depth_timestamp_ranges.items()
        }

        return self.insertion_depth_timestamp_ranges

    # parse_data_insertion_depths

    def parse_data_unique_poses(self):
        """ Parse the data based on unique robot poses"""
        unique_robot_poses : Dict[str, np.ndarray] = dict()

        tolerance = 1e-6
        
        self.unique_robot_poses_timestamp_ranges = self.identify_unique_robot_poses(
            time_in_pose_seconds=3.0,
            pose_tolerance=tolerance,
        )

        for msg, (ts_lo, ts_hi) in self.unique_robot_poses_timestamp_ranges.items():
            msg: PoseStamped
            insertion_depth = self.get_insertion_depth(msg)

            for target_depth in self.insertion_depths:
                if abs(target_depth - insertion_depth) > tolerance:
                    continue
                
                tf = np.eye(4)
                tf[:3, 3], tf[:3, :3] = nsp_util.msg2pose(msg.pose)

                pose_key = "_".join(
                    map(
                        str,
                        np.round(tf[:3, 3], tolerance)
                    )
                )
                unique_robot_poses[pose_key] = tf
                self.trial
                
                break

            # for
        # for
            
        self.unique_robot_poses = unique_robot_poses

        return self.unique_robot_poses

    # parse_data_unique_poses

    def plot_results(self, odir: str = None, show: bool = False):
        fig, ax = plt.subplots()

        ts_topic_msgs: List[Tuple[int, str, PoseStamped]] = self.get_all_messages(self.key_topic)

        ts_stage_poses = np.stack(
            [
                [ts, msg.pose.position.x, msg.pose.position.y, msg.pose.position.z]
                for ts, _, msg in ts_topic_msgs
            ],
            axis=0
        )
        ts          = ts_stage_poses[:, 0]
        stage_poses = ts_stage_poses[:, 1:]

        ax.plot(ts, stage_poses, '.')
        ax.legend(["x", "y", "z"])
        ax.set_xlabel("timestamp")
        ax.set_ylabel("Stage Axis Position")
        ax.set_title(f"Robot Stage Needle Pose: {self.key_topic}")

        if odir is not None:
            os.makedirs(odir, exist_ok=True)
            out_file = os.path.join(odir, "robot_data_stage_poses.png")
            fig.savefig(out_file)
            print("Saved figure robot data to:", out_file)

            out_file = os.path.join(odir, "robot_data_stage_poses.csv")
            pd.DataFrame(
                ts_stage_poses,
                columns=["timestamp", "x", "y", "z"],
                index=None,
            ).to_csv(
                out_file,
                header=True,
                index=False,
            )
            print("Saved all robot data to:", out_file)

        # if

        if show:
            plt.show()

    # plot_results

    def save_results(self, odir: str, filename: str = None):
        assert self.is_parsed(), (
            "Robot data has not been parsed yet."
        )
        filename = filename if filename is not None else "robot_timestamp_ranges.csv"

        ts_ranges_df = pd.DataFrame.from_dict(
            self.insertion_depth_timestamp_ranges,
            orient='index',
            columns=['ts_min', 'ts_max'],
        )
        ts_ranges_df["delta ts"] = ts_ranges_df["ts_max"] - ts_ranges_df["ts_min"]

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

        # save the needle stage pose for each of the separated insertion depths
        for trial_key, pose in self.trial_key_pose_dict.items():
            sub_odir = os.path.join(odir, str(trial_key))
            os.makedirs(sub_odir, exist_ok=True)

            pose_df = pd.DataFrame(pose)

            pose_df.to_csv(
                os.path.join(sub_odir, "robot_pose.csv"),
                header=False,
                index=False,
            )
            print(
                f"Saved robot pose for insertion depth {trial_key} mm to:",
                os.path.join(sub_odir, "robot_pose.csv")
            )

        # for
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
        "/needle/state/curvatures",
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

    def is_parsed(self):
        parsed = False
        if len(self.needle_data) > 0:
            parsed = True

        return parsed

    # is_parsed

    def determine_timestamps(self, inplace: bool=False):
        assert self.timestamp_ranges is not None, (
            "You must configure the timestamp ranges for each of the insertion depths in order to "
            "determine the timestamps"
        )

        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        kappac_topic = self.get_topics_by_name("state/kappac")[0]
        for trial_key, ts_range in self.timestamp_ranges.items():
            kappac_msgs = self.get_all_messages(kappac_topic, ts_range=ts_range)

            viable_ts_msgs = list()
            for ts, _, kc_msg in kappac_msgs:
                kc_msg: Float64MultiArray

                if all(kc == 0 for kc in kc_msg.data): # try to skip all zero kappa c
                    continue

                viable_ts_msgs.append((ts, kc_msg))

            # for
            best_ts = self.determine_median_timestamp(
                [ts for ts, *_ in kappac_msgs]
            )
            if len(viable_ts_msgs) > 0:
                best_ts = self.determine_median_timestamp(
                    [ts for ts, *_ in  viable_ts_msgs]
                )

            # if
            else:
                warnings.warn(f"Experiment trial key {trial_key} has kappa_c = 0 for all messages.")

            timestamps[trial_key] = best_ts

        # for

        return timestamps

    # determine_timestamps

    def parse_data(self):
        assert self.timestamps is not None, (
            "You must determine which timestamps to use for each of the insertions depths in order to "
            "parse the needle shape data"
        )

        # topic names
        kappac_topic = self.get_topics_by_name("state/kappac")[0]
        winit_topic  = self.get_topics_by_name("state/winit")[0]
        shape_topic  = self.get_topics_by_name("state/current_shape")[0]
        curv_topic   = self.get_topics_by_name("state/curvatures")[0]

        # parse the needle shape data
        for trial_key, ts_range in self.timestamp_ranges.items():
            target_ts = self.timestamps[trial_key]

            # parse the needle's curvatures
            closest_curv_ts, best_curv_msg = self.get_closest_message_to_timestamp(
                curv_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_curv = None
            if best_curv_msg is not None:
                best_curv_msg: Float64MultiArray
                best_curv = np.asarray(best_curv_msg.data)

            # if

            self.needle_data[trial_key]["data"]["curvature"]       = best_curv
            self.needle_data[trial_key]["timestamps"]["curvature"] = closest_curv_ts

            # parse the needle's kappa c
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

            self.needle_data[trial_key]["data"]["kappa_c"]       = best_kc
            self.needle_data[trial_key]["timestamps"]["kappa_c"] = closest_kc_ts

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

            self.needle_data[trial_key]["data"]["winit"]       = best_winit
            self.needle_data[trial_key]["timestamps"]["winit"] = closest_winit_ts

            # parse the needle's shape
            closest_shape_ts, best_shape_msg = self.get_closest_message_to_timestamp(
                shape_topic,
                target_ts,
                ts_range=ts_range
            )
            best_shape = None
            if best_shape_msg is not None:
                best_shape_msg: PoseArray
                best_shape = nsp_util.msg2poses(best_shape_msg)[:, :3, 3] # take only the positions

            # if

            self.needle_data[trial_key]["data"]["shape"]       = best_shape
            self.needle_data[trial_key]["timestamps"]["shape"] = closest_shape_ts

        # for

        return self.needle_data

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        assert len(self.needle_data) > 0, (
            "Needle data needs to be parsed first!"
        )

        filename = filename if filename is not None else "needle_data.xlsx"

        for trial_key, data_ts_dict in self.needle_data.items():
            sub_odir = os.path.join(odir, str(trial_key))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename), engine='xlsxwriter') as xl_writer:
                # write the timestamp information (timestamps grabbed)
                ts_df = pd.DataFrame.from_dict(data_ts_dict["timestamps"], orient='index')
                ts_df.loc["target"]    = self.timestamps[trial_key]
                ts_df.loc["range_min"] = self.timestamp_ranges[trial_key][0]
                ts_df.loc["range_max"] = self.timestamp_ranges[trial_key][1]

                ts_df.to_excel(
                    xl_writer,
                    sheet_name="ROS timestamps",
                    index=True,
                    header=False,
                )

                # write the data values
                for data_name, data in data_ts_dict["data"].items():
                    df = pd.DataFrame(data)
                    df.to_excel(
                        xl_writer,
                        sheet_name=data_name,
                        index=False,
                        header=False,
                    )

                # for
            # with

            print(
                f"Saved needle shape data for insertion depth {trial_key} mm to:",
                os.path.join(sub_odir, filename)
            )

        # for

    # save_results

# class: NeedleDataBagParser

class FBGSensorDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = [
        "/needle/sensor/raw",
        "/needle/sensor/processed",
        "/needle/state/curvatures",
    ]

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: List[str] = None,
        bag_db: sqlite3.Connection = None
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else FBGSensorDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
        )

        # parsed data results
        # self.fbg_sensor_data is a dict of arrays with (ts, wavelengths (nm)) per row
        self.fbg_sensor_data = defaultdict(
            lambda: {
                "raw": None,
                "processed": None,
            },
        )

    # __init__

    def is_parsed(self):
        parsed = False
        if len(self.fbg_sensor_data) > 0:
            parsed = True

        return parsed

    # is_parsed

    def determine_timestamps(self, inplace: bool = False):
        assert self.timestamp_ranges is not None, "Timestamp ranges need to be configured!"
        return self.timestamp_ranges

    # determine_timestamps

    def parse_data(self):
        raw_topic = self.get_topics_by_name("sensor/raw")[0]
        prc_topic = self.get_topics_by_name("sensor/processed")[0]
        crv_topic = self.get_topics_by_name("state/curvatures")[0]

        for depth in self.insertion_depths:
            ts_range = self.timestamp_ranges[depth]

            # get the timestamp and messages
            raw_ts_topic_msg = self.get_messages(
                topic_name=raw_topic,
                ts_range=ts_range,
                generator_count=1,
            )
            prc_ts_topic_msg = self.get_messages(
                topic_name=prc_topic,
                ts_range=ts_range,
                generator_count=1,
            )
            crv_ts_topic_msg = self.get_messages(
                topic_name=crv_topic,
                ts_range=ts_range,
                generator_count=1,
            )

            # parse the results
            self.fbg_sensor_data[depth]["raw"] = np.stack(
                [ np.append(ts, msg.data) for ts, _, msg in raw_ts_topic_msg],
                axis=0
            )
            try:
                self.fbg_sensor_data[depth]["processed"] = np.stack(
                    [ np.append(ts, msg.data) for ts, _, msg in prc_ts_topic_msg ],
                    axis=0
                )
            except ValueError:
                self.fbg_sensor_data[depth]["processed"] = np.zeros_like(self.fbg_sensor_data[depth]["raw"])

            self.fbg_sensor_data[depth]["curvatures"] = np.stack(
                [ np.append(ts, msg.data) for ts, _, msg in crv_ts_topic_msg ],
                axis=0
            )

        # for

        return self.fbg_sensor_data

    # parse_data

    def plot_results(self, odir: str = None, show: bool = False):
        raw_topic = self.get_topics_by_name("sensor/raw")[0]
        prc_topic = self.get_topics_by_name("sensor/processed")[0]
        crv_topic = self.get_topics_by_name("state/curvatures")[0]

        # get the messages and stack into usable arrays
        raw_ts_msgs: List[Tuple[int, str, Float64MultiArray]] = self.get_all_messages(raw_topic)
        prc_ts_msgs: List[Tuple[int, str, Float64MultiArray]] = self.get_all_messages(prc_topic)
        crv_ts_msgs: List[Tuple[int, str, Float64MultiArray]] = self.get_all_messages(crv_topic)

        raw_ts_wavlengths = np.stack(
            [
                [ts, *msg.data]
                for ts, _, msg in raw_ts_msgs
            ],
            axis=0
        )
        prc_ts_wavlengths = np.copy(raw_ts_wavlengths[0:1])
        prc_ts_wavlengths[:, 1:] = 0 # set to zero
        if len(prc_ts_msgs) > 0:
            prc_ts_wavlengths = np.stack(
                [
                    [ts, *msg.data]
                    for ts, _, msg in prc_ts_msgs
                ],
                axis=0
            )

        # if
        ts_curvatures = np.stack(
            [
                [ts, *msg.data]
                for ts, _, msg in crv_ts_msgs
            ],
            axis=0
        )

        # number of FBG active areas and channels
        num_aas = int((ts_curvatures.shape[1] - 1)/2)
        num_chs = (raw_ts_wavlengths.shape[1] - 1) // num_aas
        ch_aa_names, ch_names, aa_names = nss.sensorized_needles.FBGNeedle.generate_ch_aa(num_chs, num_aas)

        # plot the wavelengths
        fig_wls, axs_wls = plt.subplots(
            nrows=2,
            ncols=num_aas,
            sharex=True,
            figsize=(18, 12),
        )

        for aa_i in range(num_aas):
            mask_aa_i = np.asarray([False] + [aa_names[aa_i] in ch_aa for ch_aa in ch_aa_names])

            axs_wls[0, aa_i].plot(
                raw_ts_wavlengths[:, 0],
                raw_ts_wavlengths[:, mask_aa_i],
                '.'
            )
            axs_wls[0, aa_i].legend(ch_names)
            axs_wls[0, aa_i].set_title(aa_names[aa_i])

            axs_wls[1, aa_i].plot(
                prc_ts_wavlengths[:, 0],
                prc_ts_wavlengths[:, mask_aa_i],
                '.'
            )
            axs_wls[1, aa_i].legend(ch_names)
            axs_wls[1, aa_i].set_xlabel("Timestamps")

            axs_wls[1, 0].get_shared_y_axes().join(axs_wls[1, 0], axs_wls[1, aa_i])

        # for
        axs_wls[0, 0].set_ylabel("Raw Wavelengths (nm)")
        axs_wls[1, 0].set_ylabel("Processed Wavelengths (nm)")

        fig_wls.suptitle(f"Wavelengths of FBG Sensors: {raw_topic} & {prc_topic}")

        # plot curvatures
        fig_crvs, axs_crvs = plt.subplots(
            nrows=2,
            ncols=1,
            sharex=True,
            figsize=(10, 8),
        )

        axs_crvs[0].plot(ts_curvatures[:, 0], ts_curvatures[:, 1::2], '.')
        axs_crvs[0].set_ylabel("X Curvature")
        axs_crvs[0].legend(ch_names)

        axs_crvs[1].plot(ts_curvatures[:, 0], ts_curvatures[:, 2::2], '.')
        axs_crvs[1].set_ylabel("Y Curvature")
        axs_crvs[1].legend(ch_names)

        fig_crvs.suptitle(f"Curvatures Sensed by FBG Sensors: {crv_topic}")

        if odir is not None:
            os.makedirs(odir, exist_ok=True)
            out_file_wls = os.path.join(odir, "sensor_data_wavelengths.png")
            fig_wls.savefig(out_file_wls)
            print("Saved figure of wavelengths to:", out_file_wls)

            out_file_crvs = os.path.join(odir, "sensor_data_curvatures.png")
            fig_crvs.savefig(out_file_crvs)
            print("Saved figure of curvatures to:", out_file_crvs)

        # if

        if show:
            plt.show()

    # plot_results

    def save_results(self, odir: str, filename: str = None):
        if filename is None:
            filename = "fbg_sensor_data.xlsx"

        elif not filename.endswith(".xlsx"):
            filename += ".xlsx"

        for depth, data in self.fbg_sensor_data.items():
            data: Dict[str, np.ndarray]
            sub_odir = os.path.join(odir, str(depth))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename), engine='xlsxwriter') as xl_writer:
                raw_df = pd.DataFrame(
                    data["raw"],
                    columns=["timestamp"] + [
                        f"wavelength {i}" for i in range(1, data["raw"].shape[1])
                    ],
                ).set_index("timestamp")

                raw_df.to_excel(
                    xl_writer,
                    sheet_name="raw wavelengths",
                    index=True,
                    header=True,
                    columns=raw_df.columns,
                )

                prc_df = pd.DataFrame(
                    data["processed"],
                    columns=["timestamp"] + [
                        f"wavelength shift {i}" for i in range(1, data["processed"].shape[1])
                    ],
                ).set_index("timestamp")

                prc_df.to_excel(
                    xl_writer,
                    sheet_name="processed wavelengths",
                    index=True,
                    header=True,
                    columns=prc_df.columns,
                )

                crv_df = pd.DataFrame(
                    data["curvatures"],
                    columns=["timestamp"] + [
                        f"curvature {i//2} {'x' if i % 2 == 0 else 'y'}"
                        for i in range(data["curvatures"].shape[1]-1)
                    ],
                ).set_index("timestamp")

                crv_df.to_excel(
                    xl_writer,
                    sheet_name="curvatures",
                    index=True,
                    header=True,
                    columns=crv_df.columns,
                )

            # with

            print(
                f"Saved FBG sensor data for insertion depth {depth} mm to:",
                os.path.join(sub_odir, filename)
            )

        # for

    # save_results

# class: FBGSensorDataBagParser

class InsertionPointDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = [
        "/needle/state/skin_entry"
    ]

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: List[str] = None,
        bag_db: sqlite3.Connection = None,
        use_insertion_depths_only: bool = True,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else InsertionPointDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )

        self.key_topic = self.get_topics_by_name("state/skin_entry")[0]
        self.insertion_point_data = defaultdict(
            lambda: {
                "data": dict(),
                "timestamps": dict(),
            },
        )

    # __init__

    def is_parsed(self):
        return len(self.insertion_point_data) > 0

    # is_parsed

    def determine_timestamps(self, inplace: bool = False):
        assert (
            self.target_timestamps is not None
            or self.timestamp_ranges is not None
        ), (
            "You must configure the target timestamps for each of the insertion depths and/or the timestamp ranges"
        )

        timestamps = dict()
        if self.target_timestamps is not None:
            timestamps = self.determine_timestamps_closest_to_target(
                self.key_topic,
                self.target_timestamps,
                ts_range=self.timestamp_ranges,
            )

        # if
        else:
            for trial_key, ts_range in self.timestamp_ranges.items():
                insertionpt_msgs = self.get_all_messages(self.key_topic, ts_range=ts_range)

                ts_depth = np.asarray([
                    ts for ts, *_ in insertionpt_msgs
                ])
                if len(ts_depth) == 0:
                    warnings.warn(f"Insertion point topic did not find any timestamps in range for depth {trial_key} mm.")
                    continue

                timestamps[trial_key] = self.determine_median_timestamp(ts_depth)

        # for

        if inplace:
            self.timestamps = timestamps

        return timestamps

    # determine_timestamps

    def parse_data(self):
        assert self.timestamps is not None, (
            "You must determine which timestamps to use for each of the insertions depths in order to "
            "parse the insertion point data"
        )

        # topic names
        insertionpt_topic = self.get_topics_by_name("state/skin_entry")[0]

        for trial_key, target_ts in self.timestamps.items():
            ts_range = self.timestamp_ranges.get(trial_key)

            closest_inspt_ts, closest_inspt_msg = self.get_closest_message_to_timestamp(
                insertionpt_topic,
                target_ts,
                ts_range=ts_range,
            )
            insertion_point = None
            if closest_inspt_msg is not None:
                closest_inspt_msg: Point
                insertion_point = np.asarray(
                    StageCoordinateConversions.RobotToStage(
                        closest_inspt_msg.x,
                        closest_inspt_msg.y,
                        closest_inspt_msg.z,
                    )
                )

            # if

            self.insertion_point_data[trial_key]["data"]["insertion_point"]       = insertion_point
            self.insertion_point_data[trial_key]["timestamps"]["insertion_point"] = closest_inspt_ts

        # for

        return self.insertion_point_data

    # parse_data

    def plot_results(self, odir: str = None, show: bool = False):
        fig, ax = plt.subplots()

        ts_topic_msgs: List[Tuple[int, str, Point]] = self.get_all_messages(self.key_topic)

        ts_insertion_points = np.stack(
            [
                [ts, *StageCoordinateConversions.RobotToStage(msg.x, msg.y, msg.z)]
                for ts, _, msg in ts_topic_msgs
            ],
            axis=0,
        )
        ts     = ts_insertion_points[:, 0]
        points = ts_insertion_points[:, 1:]

        ax.plot(ts, points, '.')
        ax.legend(["x", "y", "z"])
        ax.set_xlabel("timestamp")
        ax.set_ylabel("Insertion Point Location (mm)")
        ax.set_title(f"Insertion Point: {self.key_topic}")

        if odir is not None:
            os.makedirs(odir, exist_ok=True)
            out_file = os.path.join(odir, "insertion_points.png")
            fig.savefig(out_file)
            print("Saved figure insertion point data to:", out_file)

            out_file = os.path.join(odir, "insertion_points.csv")
            pd.DataFrame(
                ts_insertion_points,
                columns=["timestamp", "x", "y", "z"],
                index=None,
            ).to_csv(
                out_file,
                header=True,
                index=False,
            )
            print("Saved all insertion point data to:", out_file)

        # if

        if show:
            plt.show()

    # plot_results

    def save_results(self, odir: str, filename: str = None):
        filename = filename if filename is not None else "insertion_point_data.xlsx"

        for trial_key, data_ts_dict in self.insertion_point_data.items():
            sub_odir = os.path.join(odir, str(trial_key))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename)) as xl_writer:
                ts_df = pd.DataFrame.from_dict(data_ts_dict["timestamps"], orient='index')
                ts_df.loc["target"]    = self.timestamps[trial_key]
                ts_df.loc["range_min"] = self.timestamp_ranges[trial_key][0]
                ts_df.loc["range_max"] = self.timestamp_ranges[trial_key][1]

                ts_df.to_excel(
                    xl_writer,
                    sheet_name="ROS timestamps",
                    index=True,
                    header=False,
                )

                # write the insertion point
                insertion_point = data_ts_dict["data"].get("insertion_point", None)
                if insertion_point is not None:
                    df = pd.Series(insertion_point, index=["x", "y", "z"])
                    df.to_excel(
                        xl_writer,
                        sheet_name="insertion_point",
                        index=True,
                        header=False,
                    )

                # if

            # with
            print(
                f"Saved insertion point data for insertion depth {trial_key} mm to:",
                os.path.join(sub_odir, filename)
            )

        # for

    # save_results

# class: InsertionPointDataBagParser
class InsertionExperimentBagParser( ExperimentBagParser ):
    DEFAULT_TOPICS_OF_INTEREST = {
        "camera"         : CameraDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "robot"          : RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "needle"         : NeedleDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "fbg"            : FBGSensorDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "insertion-point": InsertionPointDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
    }

    def __init__(
        self,
        bagdir: str,
        insertion_depths: List[float],
        bagfile: str = None,
        yamlfile: str = None,
        topics: Dict[str, List[str]] = None,
        bag_db: sqlite3.Connection = None,
        use_insertion_depths_only: bool = True,
    ):
        super().__init__(
            bagdir,
            insertion_depths=insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=topics if topics is not None else InsertionExperimentBagParser.DEFAULT_TOPICS_OF_INTEREST,
            bag_db=bag_db,
            use_insertion_depths_only=use_insertion_depths_only
        )

        self.fbg_parser = FBGSensorDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["fbg"],
            bag_db=self.bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )
        self.needle_parser = NeedleDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["needle"],
            bag_db=self.bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )
        self.robot_parser = RobotDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["robot"],
            bag_db=self.bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )
        self.camera_parser = CameraDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["camera"],
            bag_db=self.bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )
        self.insertion_point_parser = InsertionPointDataBagParser(
            bagdir,
            insertion_depths=self.insertion_depths,
            bagfile=bagfile,
            yamlfile=yamlfile,
            topics=self.topics_of_interest["insertion-point"],
            bag_db=self.bag_db,
            use_insertion_depths_only=use_insertion_depths_only,
        )

        self._to_parse = {
            "robot"          : False,
            "camera"         : False,
            "needle"         : False,
            "fbg"            : False,
            "insertion-point": False,
        }

    # __init__

    def configure(
        self,
        parse_robot:           bool = False,
        parse_camera:          bool = False,
        parse_needle:          bool = False,
        parse_fbg:             bool = False,
        parse_insertion_point: bool = False,
    ):
        if parse_camera:
            assert parse_robot, "Need to parse robot data if parsing camera data"

        self._to_parse["robot"]           = parse_robot
        self._to_parse["camera"]          = parse_camera
        self._to_parse["needle"]          = parse_needle
        self._to_parse["fbg"]             = parse_fbg
        self._to_parse["insertion-point"] = parse_insertion_point


    # configure

    def get_parsers(self) -> Dict[str, ExperimentBagParser]:
        return {
            "robot"          : self.robot_parser,
            "camera"         : self.camera_parser,
            "needle"         : self.needle_parser,
            "fbg"            : self.fbg_parser,
            "insertion-point": self.insertion_point_parser,
        }

    # get_parsers

    def parse_data(self):
        robot_timestamp_ranges = {
            depth: (None, None)
            for depth in self.insertion_depths
        }
        if self._to_parse["robot"]:
            robot_timestamp_ranges = self.robot_parser.parse_data()
            self.configure_trial_keys(self.robot_parser.trial_keys)

        # if: robot

        needle_data       = None
        needle_timestamps = None
        if self._to_parse["needle"]:
            self.needle_parser.configure_trial_keys(self.trial_keys)
            self.needle_parser.configure_timestamp_ranges(robot_timestamp_ranges)

            needle_timestamps = self.needle_parser.determine_timestamps(inplace=True)

            needle_data = self.needle_parser.parse_data()

        # if: needle

        fbg_data       = None
        fbg_timestamps = None
        if self._to_parse["fbg"]:
            self.fbg_parser.configure_trial_keys(self.trial_keys)
            self.fbg_parser.configure_timestamp_ranges(robot_timestamp_ranges)

            fbg_timestamps = self.fbg_parser.determine_timestamps(inplace=True)

            fbg_data = self.fbg_parser.parse_data()

        # if: fbg

        camera_data       = None
        camera_timestamps = None
        if self._to_parse["camera"]:
            self.camera_parser.configure_trial_keys(self.trial_keys)
            self.camera_parser.configure_timestamp_ranges(robot_timestamp_ranges)
            if needle_timestamps is not None:
                self.camera_parser.configure_target_timestamps(needle_timestamps)

            camera_timestamps = self.camera_parser.determine_timestamps(inplace=True)

            camera_data = self.camera_parser.parse_data()

        # if: camera

        insertion_point_data       = None
        insertion_point_timestamps = None
        if self._to_parse["insertion-point"]:
            self.insertion_point_parser.configure_trial_keys(self.trial_keys)
            self.insertion_point_parser.configure_timestamp_ranges(robot_timestamp_ranges)
            if needle_timestamps is not None:
                self.camera_parser.configure_target_timestamps(needle_timestamps)

            insertion_point_timestamps = self.insertion_point_parser.determine_timestamps(inplace=True)

            insertion_point_data = self.insertion_point_parser.parse_data()

        # if

        return (
            robot_timestamp_ranges,
            needle_data,
            fbg_data,
            camera_data,
            insertion_point_data,
        )

    # parse_data

    def save_results(self, odir: str, filename: str = None):
        for key, parser in self.get_parsers().items():
            if not self._to_parse[key]:
                continue

            if not parser.is_parsed():
                warnings.warn(f"Parser {parser.__class__.__name__} is not parsed!")
                continue

            parser.save_results(odir=odir)

        # for

    # save_results

    def plot_results(self, odir: str = None, show: bool = False):
        for key, parser in self.get_parsers().items():
            if not self._to_parse[key]:
                continue

            if not parser.is_parsed():
                warnings.warn(f"Parser {parser.__class__.__name__} is not parsed! Can't plot data")
                continue

            # if

            parser.plot_results(odir=odir, show=show)

        # for

    # plot_results

# class: InsertionExperimentBag