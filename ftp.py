import os
import pysftp
import sys

import config


class FTP:
    """
    An FTP server connection object for downloading and managing files on an FTP server.
    """

    def __init__(self):
        """
        Initialize FTP connection using pysftp.
        """
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None
        self.ftpsrv = pysftp.Connection(
            host=config.FTP_HOST,
            username=config.FTP_USER,
            password=config.FTP_PWD,
            cnopts=self.cnopts,
        )

    def download_directory(self, remotedir, localdir):
        """
        Download all files from remote directory.

        Params:
            remotedir (str): path of the remote directory.
            localdir (str): path of the local directory.

        Return:
            none
        """
        self.ftpsrv.get_d(remotedir, localdir, preserve_mtime=True)
