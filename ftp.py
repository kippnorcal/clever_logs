import os
import pysftp
import sys


class FTP:
    """
    An FTP server connection object for downloading and managing files on an FTP server.
    """

    def __init__(self, localdir):
        """
        Initialize FTP connection using pysftp.
        """
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None
        self.ftpsrv = pysftp.Connection(
            host=os.getenv("FTP_HOST"),
            username=os.getenv("FTP_USER"),
            password=os.getenv("FTP_PW"),
            cnopts=self.cnopts,
        )
        self.localdir = localdir

    def download_files(self, remotedir):
        """
        Download all files from remote directory.

        Params:
            remotedir (str): path of the remote directory.

        Return:
            none
        """
        self.ftpsrv.get_d(remotedir, self.localdir, preserve_mtime=True)
