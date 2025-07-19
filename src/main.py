"""Module main.py"""
import logging
import os
import sys
import argparse
import boto3


def main():
    """
    Entry point
    """

    # Logging
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info(attributes)

    # Steps
    src.data.interface.Interface(
        connector=connector, service=service, s3_parameters=s3_parameters, attributes=attributes).exc()
    src.transfer.interface.Interface(service=service, s3_parameters=s3_parameters).exc()

    # Deleting __pycache__
    src.functions.cache.Cache().exc()


if __name__ == '__main__':

    # Setting-up
    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import src.data.interface
    import src.elements.s3_parameters as s3p
    import src.elements.service as sr
    import src.functions.cache
    import src.preface.interface
    import src.transfer.interface
    import src.specific

    specific = src.specific.Specific()
    parser = argparse.ArgumentParser()
    parser.add_argument('--codes', type=specific.codes,
                        help='Expects a string of one or more comma separated gauge time series codes.')
    args = parser.parse_args()

    connector: boto3.session.Session
    s3_parameters: s3p.S3Parameters
    service: sr.Service
    attributes: dict
    connector, s3_parameters, service, attributes = src.preface.interface.Interface().exc(codes=args.codes)

    main()
