#!/usr/bin/env jython

import logging
import ConfigParser
from kahuna.session import ContextLoader
from kahuna.config import ConfigLoader
from kahuna.utils.prettyprint import pprint_machines,pprint_datastores
from physicalmachine.pmmanager import Manager
from optparse import OptionParser
from org.jclouds.abiquo.domain.exception import AbiquoException
from org.jclouds.abiquo.domain.infrastructure import Datacenter,Rack
from org.jclouds.abiquo.predicates.infrastructure import MachinePredicates,DatacenterPredicates,RackPredicates
from org.jclouds.abiquo.reference import AbiquoEdition
from org.jclouds.rest import AuthorizationException
from org.jclouds.http import HttpResponseException
from com.abiquo.model.enumerator import HypervisorType,RemoteServiceType


log = logging.getLogger("kahuna")

class MachinePlugin:
    """ Physical machines plugin. """
    def __init__(self):
        self.__config = ConfigLoader().load("machine.conf","config/machine.conf")
        self.__manager = Manager(self.__config,log)
        pass

    def commands(self):
        """ Returns the commands provided by the plugin, mapped to the handler methods. """
        commands = {}
        commands['check'] = self.check_machines
        commands['create'] = self.create_machine
        commands['delete'] = self.delete_machine
        commands['list'] = self.list_machines
        commands['datastores'] = self.list_datastores
        return commands

    def check_machines(self, args):
        """ Check state from physical machine. """
        parser = OptionParser(usage="machine check <options>")
        parser.add_option("-n","--name",help="the name of the physical machine",action="store",dest="name")
        parser.add_option("-i","--host",help="the ip of the physical machine",action="store",dest="host")
        parser.add_option("-a","--all",help="check all machines",action="store_true",dest="a")
        (options, args) = parser.parse_args(args)
        all_true = options.a
        name = options.name
        host = options.host

        if not name and not host and not all_true:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            admin =  context.getAdministrationService()
            if all_true:
                machines = admin.listMachines()
                log.debug("%s machines found." % str(len(machines)))
                [self.__manager.check_machine for machine in machines]
                pprint_machines(machines)
            else:
                if name:
                    machine = admin.findMachine(MachinePredicates.name(name))
                else:
                    machine = admin.findMachine(MachinePredicates.ip(host))
                self.__manager.check_machine(machine)
                pprint_machines([machine]);
        except (AbiquoException, AuthorizationException), ex:
            print "Error %s" % ex.getMessage()
        finally:
            context.close()

    def create_machine(self, args):
        """ Create a physical machine in abiquo. This method uses configurable constats for default values."""
        parser = OptionParser(usage="machine create --host <host> <options>")

        # create options
        parser.add_option("-i","--host",
                help="ip or hostname from machine to create in abiquo [required]",action="store",dest="host")
        parser.add_option("-u","--user",help="user to loggin in the machine",action="store",dest="user")
        parser.add_option("-p","--psswd",help="password to loggin in the machine",action="store",dest="psswd")
        parser.add_option("-t","--type",help="hypervisor type of the machine",action="store",dest="type")
        parser.add_option("-r","--rsip",help="ip from remote services",action="store",dest="remoteservicesip")
        parser.add_option("-d","--datastore",
                help="datastore to enable on physical machine",action="store",dest="datastore")
        parser.add_option("-s","--vswitch",
                help="virtual switch to select on physical machine",action="store",dest="vswitch")
        (options, args) = parser.parse_args(args)
        
        # parse options
        host = options.host
        if not host:
            parser.print_help()
            return

        user = self.__manager.get_config(options,host,"user")
        psswd = self.__manager.get_config(options,host,"psswd")
        rsip = self.__manager.get_config(options,host,"remoteservicesip")
        dsname = self.__manager.get_config(options,host,"datastore")
        vswitch =  self.__manager.get_config(options,host,"vswitch")
        hypervisor = self.__manager.get_config(options,host,"type",False)

        context = ContextLoader().load()
        try:
            admin = context.getAdministrationService()

            # search or create datacenter
            log.debug("Searching for the datacenter 'kahuna' with remote services ip '%s'." % rsip)
            dcs = admin.listDatacenters()
            dc = self.__manager.get_datacenter_by_rsip(dcs, rsip, context)

            # discover machine
            hypTypes = [HypervisorType.valueOf(hypervisor)] if hypervisor else HypervisorType.values()

            machine = None
            for hyp in hypTypes:
                try:
                    log.debug("Trying hypervisor %s" % hyp.name())
                    machine = dc.discoverSingleMachine(host, hyp, user, psswd)
                    break
                except (AbiquoException, HttpResponseException), ex:
                    if type(ex) is AbiquoException and (ex.hasError("NC-3") or ex.hasError("RS-2")):
                        print ex.getMessage().replace("\n","")
                        return
                    log.debug(ex.getMessage().replace("\n",""))

            if not machine:
                print "Not machine found in %s" % host
                return

            log.debug("Machine %s of type %s found" % (machine.getName(), machine.getType().name()))

            # enabling datastore
            ds = machine.findDatastore(dsname)
            if not ds:
                print "Missing datastore %s in machine" % dsname
                return
            ds.setEnabled(True)

            # setting virtual switch
            vs=machine.findAvailableVirtualSwitch(vswitch)
            if not vs:
                print "Missing virtual switch %s in machine" % vswitch
                return

            # saving machine
            machine.setRack(dc.findRack(RackPredicates.name('Volcano')))
            machine.setVirtualSwitch(vs)
            machine.save()
            log.debug("Machine saved")
            pprint_machines([machine])

        except (AbiquoException,AuthorizationException), ex:
            if ex.hasError("HYPERVISOR-1") or ex.hasError("HYPERVISOR-2"):
                print "Error: Machine already exists"
            else:
                print "Error: %s " % ex.getMessage()
        finally:
            context.close()

    def delete_machine(self, args):
        """ Remove a physical machine from abiquo. """
        parser = OptionParser(usage="machine delete <options>")
        parser.add_option("-n","--name",help="the name of the physical machine",action="store",dest="name")
        parser.add_option("-i","--host",help="the ip of the physical machine",action="store",dest="host")
        parser.add_option("-a","--all",help="afects all physical machines in abiquo",action="store_true",dest="all_true")
        (options, args) = parser.parse_args(args)
        name = options.name
        host = options.host
        all_true = options.all_true

        if not all_true and not name and not host:
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            admin =  context.getAdministrationService()

            if all_true:
                machines = admin.listMachines()
                if not machines:
                    print "Not machines found"
                    return
            else:
                if name:
                    machine = admin.findMachine(MachinePredicates.name(name))
                else:
                    machine = admin.findMachine(MachinePredicates.ip(host))
                if not machine:
                    print "Machine not found"
                    return
                machines = [machine]
            
            for machine in machines:
                name=machine.getName()
                machine.delete()
                print "Machine '%s' deleted succesfully" % name

        except (AbiquoException, AuthorizationException), ex:
            print "Error %s" % ex.getMessage()
        finally:
            context.close()
    
    def list_machines(self,args):
        """ List physical machines from abiquo """
        context = ContextLoader().load()
        try:
            admin = context.getAdministrationService()
            machines = admin.listMachines()
            pprint_machines(machines)
        except (AbiquoException, AuthorizationException), ex:
            print "Error %s" % ex.getMessage()
        finally:
            context.close()

    def list_datastores(self,args):
        """ List all datastores from physical machine """

        parser = OptionParser(usage="machine datastores <options>")
        parser.add_option("-n","--name",help="the name of a physical machine",action="store",dest="name")
        parser.add_option("-i","--host",help="the ip of a physical machine",action="store",dest="host")
        parser.add_option("-a","--all",help="afects all physical machines in abiquo",action="store_true",dest="all_true")
        parser.add_option("-d","--datastore",help="the UUID of a datastore",action="store",dest="datastore")
        parser.add_option("--enable",help="enables the selected datastore",action="store_true",dest="enable_true")
        parser.add_option("--disable",help="disables the selected datastore",action="store_true",dest="disable_true")
        (options, args) = parser.parse_args(args)
        name = options.name
        host = options.host
        datastore = options.datastore
        all_true = options.all_true
        enable = options.enable_true
        disable = options.disable_true

        if not all_true and not name and not host and not (datastore and (enable or disable)):
            parser.print_help()
            return

        context = ContextLoader().load()
        try:
            admin =  context.getAdministrationService()

            # show datastores from all machines
            if all_true:
                machines = admin.listMachines()
                if not machines:
                    print "Not machines found"
                    return
                log.debug("%i machines found" % len(machines))
                pprint_datastores(machines)
            # show datastores from a single machine
            elif (name or host) and not datastore:
                if name:
                    machine = admin.findMachine(MachinePredicates.name(name))
                else:
                    machine = admin.findMachine(MachinePredicates.ip(host))
                if not machine:
                    print "Machine not found"
                    return
                machines = [machine]
                log.debug("%i machines found" % len(machines))
                pprint_datastores(machines)
            # enable or disable a datastore
            elif (name or host) and (datastore and (enable or disable)):
                if name:
                    machine = admin.findMachine(MachinePredicates.name(name))
                else:
                    machine = admin.findMachine(MachinePredicates.ip(host))
                if not machine:
                    print "Machine not found"
                    return
                datastores = machine.getDatastores()
                boolean = True if enable else False
                filtered = map(self__manager.enable_disable_datastore, datastores)
                print filtered
            else:
                parser.print_help()

        except (AbiquoException, AuthorizationException), ex:
            print "Error %s" % ex.getMessage()
        finally:
            context.close()


def load():
    """ Loads the current plugin. """
    return MachinePlugin()

