#!/usr/bin/env jython

import logging
from optparse import OptionParser
from kahuna.session import ContextLoader
from kahuna.utils.prettyprint import pprint_volumes
from org.jclouds.abiquo.predicates.cloud import VirtualMachinePredicates
from org.jclouds.abiquo.predicates.cloud import VolumePredicates
from org.jclouds.abiquo.domain.exception import AbiquoException
from org.jclouds.rest import AuthorizationException
from storage import helper

log = logging.getLogger('kahuna')

class VolumePlugin:
    """ Volume plugin. """
    def __init__(self):
        pass

    def commands(self):
        """ Returns the commands provided by the plugin, mapped to the handler methods. """
        commands = {}
        commands['list'] = self.list
        commands['find'] = self.find
        commands['attach'] = self.attach
        commands['detach'] = self.detach
        return commands

    def list(self, args):
        """ List all available volumes. """
        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vdcs = cloud.listVirtualDatacenters()
            volumes = []
            [volumes.extend(vdc.listVolumes()) for vdc in vdcs]
            pprint_volumes(volumes)
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def find(self, args):
        """ Find an available volume given its name. """
        # Parse user input to get the name of the volume
        parser = OptionParser(usage="volume find <options>")
        parser.add_option("-n", "--name", help="The name of the volume to find", dest="name")
        (options, args) = parser.parse_args(args)
        name = options.name
        if not name:
            parser.print_help()
            return

        # Once user input has been read, find the volume
        context = ContextLoader().load()
        try:
            volume = helper.find_volume(context, name)
            if volume:
                pprint_volumes([volume])
            else:
                print "No volume found with name: %s" % name
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def attach(self, args):
        """ Attach a volume to the given virtual machine. """
        parser = OptionParser(usage="volume attach <options>")
        parser.add_option("-n", "--name", help="The name of the volume to attach", dest="name")
        parser.add_option("-v", "--vm",
                help="The name of the virtual machine where the volume will be attached", dest="vm")
        (options, args) = parser.parse_args(args)
        if not options.name or not options.vm:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            volume = helper.find_volume(context, options.name)
            if not volume:
                print "No volume found with name: %s" % options.name
                return
            cloud = context.getCloudService()
            vm = cloud.findVirtualMachine(VirtualMachinePredicates.name(options.vm))
            if not vm:
                print "No virtual machine found with name: %s" % options.vm
                return
            
            log.debug("Attaching volume %s to %s..." % (options.name, options.vm))
            if vm.getState().existsInHypervisor():
                print "Attaching volume to a running virtual machine.",
                print "This may take some time..."
            
            vm.attachVolumes(volume)
            pprint_volumes([helper.refresh_volume(context, volume)])
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def detach(self, args):
        """ Detach a volume from the given virtual machine. """
        parser = OptionParser(usage="volume detach <options>")
        parser.add_option("-n", "--name", help="The name of the volume to detach", dest="name")
        (options, args) = parser.parse_args(args)
        if not options.name:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            volume = helper.find_volume(context, options.name)
            if not volume:
                print "No volume found with name: %s" % options.name
                return
            
            vm = helper.get_attached_vm(context, volume)
            if not vm:
                print "Volume %s is not attached to any virtual machine" % options.name
                return
            
            log.debug("Detaching volume %s from %s..." % (options.name, vm.getName()))
            if vm.getState().existsInHypervisor():
                print "Detaching volume from a running virtual machine.",
                print "This may take some time..."
            
            vm.detachVolumes(volume)
            pprint_volumes([helper.refresh_volume(context, volume)])
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()
def load():
    """ Loads the current plugin. """
    return VolumePlugin()

