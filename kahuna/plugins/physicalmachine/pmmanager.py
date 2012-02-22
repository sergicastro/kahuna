#!/usr/bin/env jython

import ConfigParser
from kahuna.config import ConfigLoader
from org.jclouds.abiquo.domain.exception import AbiquoException
from org.jclouds.abiquo.domain.infrastructure import Datacenter,Rack
from org.jclouds.abiquo.predicates.infrastructure import MachinePredicates,DatacenterPredicates
from org.jclouds.abiquo.predicates.infrastructure import RackPredicates,RemoteServicePredicates
from org.jclouds.abiquo.reference import AbiquoEdition
from org.jclouds.rest import AuthorizationException
from com.abiquo.model.enumerator import RemoteServiceType

class Manager:
    """ Physical machine manager """
    def __init__(self, config, logger):
        self.__config = config
        self.__logger = logger
    
    def get_config(self, options, host, prop, raiseerror=True):
        """ Gets a value from config or options """

        p = eval("options.%s" % prop)
        if p:
            return p
        try:
            return self.__config.get(host, prop)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            try:
                return self.__config.get("global", prop)
            except (ConfigParser.NoSectionError, ConfigParser.NoOptionError), ex:
                if raiseerror:
                    raise ex
                else:
                    return

    def check_machine(self, machine):
        """ Executes the check for a physical machine  """

        try:
            if not machine:
                raise Exception("machine not found")
            state = machine.check()
            machine.setState(state)
            self.__logger.debug("%s - %s" % (machine.getName(), state))
        except (AbiquoException, AuthorizationException), ex:
            print "Error %s" % ex.getMessage()

    def get_datacenter_by_rsip(self, datacenters, rsip, context):
        """ Returns the datacenter with Node Collector and Virtual System Monitor in the given ip.
        If it is not found it will be created. """

        selected_dc = None        
        for dc in datacenters:
            nc = dc.findRemoteService(RemoteServicePredicates.type(RemoteServiceType.NODE_COLLECTOR))
            if nc and nc.getUri().find(rsip) >= 0:
                self.__logger.debug("Node Collector [%s] found in datacenter '%s'." % (rsip, dc.getName()))
                vsm = dc.findRemoteService(RemoteServicePredicates.type(RemoteServiceType.VIRTUAL_SYSTEM_MONITOR))
                if vsm and vsm.getUri().find(rsip) >= 0:
                    self.__logger.debug("Virtual System Monitor [%s] found in datacenter '%s'." % (rsip, dc.getName()))
                    selected_dc = dc
                    break
        
        if not selected_dc:
            self.__logger.debug("No datacenter with NC and VSM in '%s' found." % rsip)
            dc = Datacenter.builder(context) \
                    .name('Kahuna') \
                    .location('Kapapala') \
                    .remoteServices(rsip,AbiquoEdition.ENTERPRISE) \
                    .build()
            try:
                dc.save()
            except (AbiquoException), ex:
                if ex.hasError("RS-3"):
                    print "Ip %s to create remote services has been used yet, try with another one" % rsip
                    dc.delete()
                    return
                else:
                    raise ex

            rack = Rack.builder(context,dc).name('Volcano').build()
            rack.save()
            self.__logger.debug("New datacenter '%s' created." % dc.getName())
            return dc
        else:
            rack = selected_dc.findRack(RackPredicates.name('Volcano'))
            if not rack:
                rack = Rack.builder(context,selected_dc).name('Volcano').build()
                rack.save()
            self.__logger.debug("Datacenter '%s' found. " % selected_dc.getName())
            return selected_dc
            
    def enable_disable_datastore(self, datastore, uuid, boolean_value):
        """ Enables or disables the given datastore. """
        if datastore.getDatastoreUUID() == uuid:
            datastore.setEnabled(boolean_value)
            self.__logger.debug("%sabling datastore '%s'" % ("en" if boolean_value else "dis", datastore.getRootPath()))
        return datastore
