import sys, os
import json
from ament_index_python.packages import get_package_share_directory

from launch import LaunchDescription, actions, conditions
from launch.substitutions.launch_configuration import LaunchConfiguration
from launch.actions import IncludeLaunchDescription, DeclareLaunchArgument
from launch.substitutions import PythonExpression, LocalSubstitution, TextSubstitution, PathJoinSubstitution
from launch.launch_description_sources import PythonLaunchDescriptionSource

from launch_ros.substitutions import FindPackageShare

pkg_fbg_interrogator = FindPackageShare('sm130_interrogator_py')
pkg_needle_shape_publisher = get_package_share_directory('needle_shape_publisher')

def determineCHsAAs(needleParamFile: str):
    """ Determine the number of channels and active areas available """
    with open(needleParamFile, 'r') as paramFile:
        params = json.load(paramFile) 

    # with
    numChs = params['# channels']
    numAAs = params['# active areas']

    return numChs, numAAs
    
# determineCHsAAs

def generate_launch_description():
    ld = LaunchDescription()

    # determine #chs and numAAs
    numCHs, numAAs = None, None
    default_needleparam_file = "needle_params_2022-12-11_Jig-Calibration.json" #"needle_params_2021-08-16_Jig-Calibration_best.json" 
    for arg in sys.argv:
        if arg.startswith("needleParamFile:="):
            needleParamFile = arg.split(":=")[1]
            break
        # if        
    # for 

    # check to see if there exists a persistent needle parameter file
    default_needleparam_file_persistent = os.path.join(
        pkg_needle_shape_publisher, 
        "needle_data", 
        default_needleparam_file.replace(".json", "-ref-wavelength-latest.json")
    )
    if os.path.isfile(default_needleparam_file_persistent):
        default_needleparam_file = default_needleparam_file_persistent

    if numCHs is None and numAAs is None: # just in-case using default value
        needleParamFile = default_needleparam_file

    numCHs, numAAs = determineCHsAAs(os.path.join(pkg_needle_shape_publisher, "needle_data", needleParamFile))
    print(f"NumChs: {numCHs} | NumAAs: {numAAs}")
    # arguments
    arg_simlevel = DeclareLaunchArgument( "sim_level_needle_sensing",
                                          default_value="1",
                                          description=("Simulation level: 1 - demo FBG interrogator, "
                                                       "2 - real FBG sensors" ))

    # - shape-sensing needle arguments
    arg_needleparams = DeclareLaunchArgument( 'needleParamFile',
                                        default_value=default_needleparam_file,
                                        description="The shape-sensing needle parameter json file." )

    arg_numsignals = DeclareLaunchArgument( 'numSignals', description="The number of FBG signals to collect.",
                                            default_value="200" )

    arg_optim_maxiter = DeclareLaunchArgument( 'optimMaxIterations', default_value="15",
                                               description="The maximum number of iterations for needle shape optimizer." )

    arg_temp_compensate = DeclareLaunchArgument( 'tempCompensate', default_value="True",
                                               description="Whether to perform temperature compensation or not." )

    # - interrogator arguments                            
    arg_interrIP = DeclareLaunchArgument('interrogatorIP', 
                                         default_value='192.168.1.11',
                                         description="IP address of the FBG interrogator." )
    arg_interrogatorparams = DeclareLaunchArgument( 'interrogatorParamFile',
                                                      default_value='',
                                                      description='Interrogator parameter file to use in sm130_interrogator local share directory. This overrides all arguments.' )

    # other launch files
    # - needle shape publisher
    ld_needlepub = IncludeLaunchDescription( # needle shape publisher
            PythonLaunchDescriptionSource(
               os.path.join(pkg_needle_shape_publisher, 'needle.launch.py')
            ),
            launch_arguments = {
                'needleParamFile'   : PathJoinSubstitution( [pkg_needle_shape_publisher, "needle_data", LaunchConfiguration( 'needleParamFile')]),
                'numSignals'        : LaunchConfiguration('numSignals'),
                'optimMaxIterations': LaunchConfiguration('optimMaxIterations'),
                'tempCompensate'    : LaunchConfiguration('tempCompensate'),
            }.items()
    )

    # - interrogator launching
    ld_interrogator = IncludeLaunchDescription(
            PythonLaunchDescriptionSource(
                PathJoinSubstitution( [pkg_fbg_interrogator, 'sm130.launch.py'] )
            ),
            launch_arguments = {
                    'sim_level_interrogator': LaunchConfiguration('sim_level_needle_sensing'),
                    'ip'                    : LaunchConfiguration('interrogatorIP'),
                    'numCH'                 : TextSubstitution(text=str(numCHs)), 
                    'numAA'                 : TextSubstitution(text=str(numAAs)),
                    'paramFile'             : LaunchConfiguration('interrogatorParamFile'),
                    'needleParamFile'       : LaunchConfiguration('needleParamFile'),
            }.items()
    )

    # add to launch description
    ld.add_action(arg_simlevel)

    ld.add_action(arg_needleparams)
    ld.add_action(arg_numsignals)
    ld.add_action(arg_optim_maxiter)
    ld.add_action(arg_temp_compensate)

    ld.add_action(arg_interrIP)
    ld.add_action(arg_interrogatorparams)
   
    ld.add_action(ld_needlepub)
    ld.add_action(ld_interrogator)
    

    return ld

# generate_launch_description