
""" CLI for the bioinfo subcommand
"""
import click
import taca.utils.bioinfo_tab as bt


@click.group()
def bioinfo_deliveries():
	""" Update statusdb with information about FC entry point """
	pass

# bioinfo subcommands
@bioinfo_deliveries.command()
@click.argument('rundir')
def updaterun(rundir):
    """saves the bioinfo data to statusdb"""
    bt.update_statusdb(rundir)

@bioinfo_deliveries.command()
def update():
    """saves the bioinfo data of everything that can be found to statusdb"""
    bt.collect_runs()
