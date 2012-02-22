#!/usr/bin/env jython

import logging
from optparse import OptionParser
from kahuna.session import ContextLoader
from kahuna.utils.prettyprint import pprint_vms
from virtualmachine import helper
from org.jclouds.abiquo.domain.cloud import VirtualAppliance
from org.jclouds.abiquo.domain.cloud import VirtualMachine
from org.jclouds.abiquo.predicates.cloud import VirtualAppliancePredicates
from org.jclouds.abiquo.predicates.cloud import VirtualMachinePredicates
from org.jclouds.abiquo.domain.exception import AbiquoException
from org.jclouds.rest import AuthorizationException

log = logging.getLogger('kahuna')


class VmPlugin:
    """ Virtual machine plugin. """
    def __init__(self):
        pass

    def commands(self):
        """ Returns the provided commands, mapped to handler methods. """
        commands = {}
        commands['list'] = self.list
        commands['find'] = self.find
        commands['deploy'] = self.deploy
        commands['undeploy'] = self.undeploy
        commands['create'] = self.create
        commands['delete'] = self.delete
        return commands

    def list(self, args):
        """ List all virtual machines. """
        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vms = cloud.listVirtualMachines()
            pprint_vms(vms)
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def find(self, args):
        """ Find a virtual machine given its name. """
        # Parse user input to get the name of the virtual machine
        parser = OptionParser(usage="vm find <options>")
        parser.add_option("-n", "--name", dest="name",
                help="The name of the virtual machine to find")
        parser.add_option("-v", "--verbose", dest="verbose",
                action="store_true",
                help="Show virtual machine extended information")
        (options, args) = parser.parse_args(args)
        name = options.name
        if not name:
            parser.print_help()
            return

        # Once user input has been read, find the virtual machine
        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vm = cloud.findVirtualMachine(VirtualMachinePredicates.name(name))
            if vm:
                pprint_vms([vm], options.verbose)
            else:
                print "No virtual machine found with name: %s" % name
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def deploy(self, args):
        """ Deploy an existing virtual machine given its name. """
        # Parse user input to get the name of the virtual machine
        parser = OptionParser(usage="vm deploy <options>")
        parser.add_option("-n", "--name", dest="name",
                help="The name of the virtual machine to deploy")
        (options, args) = parser.parse_args(args)
        name = options.name
        if not name:
            parser.print_help()
            return

        # Once user input has been read, find the VM
        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vm = cloud.findVirtualMachine(VirtualMachinePredicates.name(name))
            if vm:
                vm = helper.deploy_vm(context, vm)
                pprint_vms([vm])
            else:
                print "No virtual machine found with name: %s" % name
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def undeploy(self, args):
        """ Undeploy an existing virtual machine given its name. """
        # Parse user input to get the name of the virtual machine
        parser = OptionParser(usage="vm undeploy <options>")
        parser.add_option("-n", "--name", dest="name",
                help="The name of the virtual machine to undeploy")
        (options, args) = parser.parse_args(args)
        name = options.name
        if not name:
            parser.print_help()
            return

        # Once user input has been read, find the virtual machine
        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vm = cloud.findVirtualMachine(VirtualMachinePredicates.name(name))
            if vm:
                vm = helper.undeploy_vm(context, vm)
                pprint_vms([vm])
            else:
                print "No virtual machine found with name: %s" % name
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def create(self, args):
        """ Creates a virtual machine based on a given template. """
        # Parse user input to get the name of the virtual machine
        parser = OptionParser(usage="vm create <options>")
        parser.add_option("-t", "--template-id", dest="template",
                type="int", help="The id of the template to use")
        parser.add_option("-c", "--cpu", dest="cpu", type="int",
                help="The number of cores")
        parser.add_option("-r", "--ram", dest="ram", type="int",
                help="The RAM in MB")
        parser.add_option("-d", "--deploy", dest="deploy",
                action="store_true",
                help="Deploy the virtual machine after creating it")
        (options, args) = parser.parse_args(args)
        if not options.template:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            template = helper.find_template_by_id(context, options.template)
            if not template:
                print "No template was found with id %s" % options.template
                return
            log.debug("Using template: %s" % template.getName())

            vdc = helper.get_virtual_datacenter_for_template(context, template)
            if not vdc:
                print ("Could not create a compatible virtual datacenter "
                    "for %s") % template.getName()
                return
            log.debug("Using virtual datacenter: %s" % vdc.getName())

            name = "Kahuna-" + context.getIdentity()
            vapp = vdc.findVirtualAppliance(
                    VirtualAppliancePredicates.name(name))
            if not vapp:
                log.debug(("Virtual appliance %s not found. "
                    "Creating it...") % name)
                vapp = VirtualAppliance.builder(context, vdc) \
                        .name(name) \
                        .build()
                vapp.save()

            builder = VirtualMachine.builder(context, vapp, template)
            if options.cpu:
                builder.cpu(options.cpu)
            if options.ram:
                builder.ram(options.ram)
            vm = builder.build()
            vm.save()

            if options.deploy:
                vm = helper.deploy_vm(context, vm)

            pprint_vms([vm])
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()

    def delete(self, args):
        """ Delete a virtual machine given its name. """
        # Parse user input to get the name of the virtual machine
        parser = OptionParser(usage="vm delete <options>")
        parser.add_option("-n", "--name", dest="name",
                help="The name of the virtual machine to delete")
        parser.add_option("-u", "--undeploy", dest="undeploy",
                action="store_true",
                help="undeploy the virtual machine before deleting it")
        (options, args) = parser.parse_args(args)
        name = options.name
        if not name:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            cloud = context.getCloudService()
            vm = cloud.findVirtualMachine(VirtualMachinePredicates.name(name))
            if vm:
                state = vm.getState()
                if not options.undeploy and state.existsInHypervisor():
                    print ("Virtual machine is deployed. "
                            "Undeploy it before deleting.")
                elif options.undeploy and state.existsInHypervisor():
                    vm = helper.undeploy_vm(context, vm)
                    vm.delete()
                else:
                    vm.delete()
            else:
                print "No virtual machine found with name: %s" % name
        except (AbiquoException, AuthorizationException), ex:
            print "Error: %s" % ex.getMessage()
        finally:
            context.close()


def load():
    """ Loads the current plugin. """
    return VmPlugin()
