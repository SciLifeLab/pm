""" CLI for the storage subcommand
"""
import click
from taca.storage import storage as st


@click.group()
@click.option('-d', '--days', type=click.INT, help="Days to consider as thershold")
@click.option('-r', '--run', type=click.Path(exists=True))
@click.pass_context
def storage(ctx, days, run):
	""" 
	Storage management methods and utilities 
	"""
	pass

# Storage subcommands
@storage.command()
@click.option('--backend', type=click.Choice(['swestore']), required=True,
              help='Long term storage backend')
@click.option('-m','--max-runs', type=click.INT, 
			help='Limit the number of runs to be archived simultaneously')
@click.option('-f', '--force', is_flag=True,
			 help=("Force archiving even if the run is not complete (not RTAComplete.txt file found)"))
@click.option('-c', '--compress-only', is_flag=True, help='Only compress the run without archiving it')
@click.pass_context
def archive(ctx, backend, max_runs, force, compress_only):
    """ Archive old runs to SWESTORE
	"""
    params = ctx.parent.params
    if backend == 'swestore':
        st.archive_to_swestore(days=params.get('days'), run=params.get('run'), max_runs=max_runs,
							   force=force, compress_only=compress_only)

@storage.command()
@click.option('-s','--site', 
			type=click.Choice(['swestore','archive','illumina','analysis','nas','processing-server']),
            required=True, help='Site to perform cleanup')
@click.option('-n','--dry-run', is_flag=True, help='Perform dry run i.e. Executes nothing but log')
@click.pass_context
def cleanup(ctx, site, dry_run):
    """ Do appropriate cleanup on the given site i.e. NAS/processing servers/UPPMAX """
    params = ctx.parent.params
    days = params.get('days')
    if site == 'nas':
        st.cleanup_nas(days)
    if site == 'processing-server':
        st.cleanup_processing(days)
    if site == 'swestore':
        st.cleanup_swestore(days, dry_run)
    if site in ['illumina','analysis','archive']:
        st.cleanup_uppmax(site, days, dry_run)
