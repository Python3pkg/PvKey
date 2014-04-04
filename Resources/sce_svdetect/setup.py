"""
Does stuff I do to setup SVdetect.

author: Ettore Rizzo
"""
from starcluster.logger import log
from sce import PluginSetup
from sce import utils
from sce import session

import os
opj = os.path.join


class Setup(PluginSetup):

    def run2(self):
        log.info('Running SVdetect')
        master = self.master
        utils.apt_update(master)

        #used by Ettore for SVdetect
        log.info("Installing SVdetect required perl libraries.")
        for node in self.nodes:
            self.on_add_node2(node)
	
    def on_add_node2(self, node):
        #log.info('Installing cpanminus')
        node.ssh.execute('apt-get --yes --force-yes install cpanminus')
	#log.info('Installing perl packages')
	node.ssh.execute('cpanm Config::General')
	node.ssh.execute('cpanm Tie::IxHash')
	node.ssh.execute('cpanm Parallel::ForkManager')
   
