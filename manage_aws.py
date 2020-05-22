import pandas as pd
import boto3
import json
import time
import configparser
import argparse
import datetime


def prettyRedshiftProps(props):
    """
    Description:
      Retrieves the clsuter properties and formats them for display

    Parameters:
      props - feed the cluster identified to the api

    Returns:
      none
    """

    pd.set_option('display.max_colwidth', -1)
    keysToShow = [
        "ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
        "DBName", "Endpoint", "NumberOfNodes", 'VpcId'
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_cluster(
    KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
    DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
):
    """
    Description:
     Configures a new AWS redshift cluster, using parameters found in dwh.cfg

     First, a new IAM role will be built to allow redshift to access other
     services.  Then, grant S3 read only access to the role.  Next, build the
     cluster and wait for it to available.  Finally, allow access to the cluster
     from all sources (can be changed to restrict access)

    Parameters:
      KEY - remote access key
      SECRET - remote access secret
      CLUSTER_TYPE - single-node or multi-node
      NUM_NODES - number of nodes to build into cluster
      NODE_TYPE - which redshift configuration (dc2.large, etc)
      CLUSTER_IDENTIFIER - prefix for cluster name
      DB - name for database
      DB_USER - user to build for db access
      DB_PASSWORD - password for db access
      PORT - port to access db
      IAM_ROLE_NAME - prefix for iam role name
      ec2 - AWS boto3 EC2 connection object
      iam - AWS boto3 IAM connection object
      redshift - AWS boto3 Redshift connection object

    Returns:
      N/A
    """

    # create the role
    try:
        print("Creating a new IAM Role...")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )
    except Exception as e:
        print(e)

    # attach the policy
    print("Attaching Policy...")
    iam.attach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    # show the IAM role ARN
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    print()

    try:
        response = redshift.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            DBName=DB,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER
    )['Clusters'][0]

    while myClusterProps['ClusterStatus'] != 'available':
        print("Waiting for cluster to become available...")
        print()
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=CLUSTER_IDENTIFIER
        )['Clusters'][0]
        print(prettyRedshiftProps(myClusterProps))
        print()
        time.sleep(15)
    else:
        print("Cluster is up and running!")
        print()
        print("Address: {}".format(myClusterProps['Endpoint']['Address']))
        print()

    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", ENDPOINT)
    print("ROLE_ARN :: ", ROLE_ARN)
    print()

    # test tcp connection into cluster
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(PORT),
            ToPort=int(PORT)
        )
    except Exception as e:
        print(e)


def delete_cluster(
    KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
    DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
):

    """
    Description:
      Removes an existing Redshift cluster, using parameters found in dwh.cfg

      First, the cluster will be shut down. Then the role policy will be detached
      from the role, then finally the role will be deleted.

    Parameters:
       KEY - remote access key
       SECRET - remote access secret
       CLUSTER_TYPE - single-node or multi-node
       NUM_NODES - number of nodes to build into cluster
       NODE_TYPE - which redshift configuration (dc2.large, etc)
       CLUSTER_IDENTIFIER - prefix for cluster name
       DB - name for database
       DB_USER - user to build for db access
       DB_PASSWORD - password for db access
       PORT - port to access db
       IAM_ROLE_NAME - prefix for iam role name
       ec2 - AWS boto3 EC2 connection object
       iam - AWS boto3 IAM connection object
       redshift - AWS boto3 Redshift connection object

    Returns:
      N/A
    """

    try:
        redshift.delete_cluster(
            ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print('Cluster does not exist or is already gone: {}'.format(e))
        return

    x = 1
    while x == 1:
        print("Waiting for cluster to go away...")
        print()
        try:
            myClusterProps = redshift.describe_clusters(
                ClusterIdentifier=CLUSTER_IDENTIFIER
            )
        except Exception as e:
            print('Cluster does not exist or is already gone: {}'.format(e))
            return
        time.sleep(15)


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    CLUSTER_TYPE = config.get("DWH", "CLUSTER_TYPE")
    NUM_NODES = config.get("DWH", "NUM_NODES")
    NODE_TYPE = config.get("DWH", "NODE_TYPE")
    CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    DB = config.get("DWH", "DB")
    DB_USER = config.get("DWH", "DB_USER")
    DB_PASSWORD = config.get("DWH", "DB_PASSWORD")
    PORT = config.get("DWH", "PORT")
    IAM_ROLE_NAME = config.get("DWH", "IAM_ROLE_NAME")

    pd.DataFrame({
        "Param":[
            "CLUSTER_TYPE", "NUM_NODES", "NODE_TYPE", "CLUSTER_IDENTIFIER",
            "DB", "DB_USER", "DB_PASSWORD", "PORT", "IAM_ROLE_NAME"
        ],
        "Value":[
            CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB,
            DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME
        ]
    })

    ec2 = boto3.resource(
        'ec2',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    iam = boto3.client(
        'iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )

    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    # Creates Argument Parser object named parser
    parser = argparse.ArgumentParser()

    # Argument 1: that's a path to a folder
    parser.add_argument('--mode', type = str, help = 'create_cluster/delete_cluster')

    # Assigns variable in_args to parse_args()
    args = parser.parse_args()

    if args.mode == 'create_cluster':
        print('Building new cluster!')
        print()
        create_cluster(
            KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
            DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
        )
    elif args.mode == 'delete_cluster':
        print('Shutting down cluster!')
        print()
        delete_cluster(
            KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
            DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
        )
    else:
        print('No mode defined, exiting!')
        print()


if __name__ == "__main__":
    start = datetime.datetime.now()
    start_dt = start.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('Starting AWS Builder script at {}'.format(start_dt))
    print()

    main()

    end = datetime.datetime.now()
    end_dt = end.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('AWS Builder script complete at {}'.format(end_dt))
    print()
    print("Total execution time: {}".format(end - start))
    print()
