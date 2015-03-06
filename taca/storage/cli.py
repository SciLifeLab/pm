""" CLI for the storage subcommand
"""
import click
from taca.storage import storage as st


@click.group()
@click.option('-d', '--days', default=2,
		      help="Days to consider a run old")
@click.option('-r', '--run', type=click.Path(exists=True))
@click.pass_context
def storage(ctx, days, run):
	""" Storage management methods and utilities """
	pass

# Storage subcommands
@storage.command()
@click.option('--backend', type=click.Choice(['swestore']), required=True,
			  help='Long term storage backend')
@click.pass_context
def archive(ctx, backend):
	""" Archive old runs to SWESTORE
	"""
	params = ctx.parent.params
	if backend == 'swestore':
		st.archive_to_swestore(days=params.get('days'), run=params.get('run'))


@storage.command()
@click.pass_context
def cleanup(ctx):
	""" Move old runs to nosync directory so they're not synced to the processing server """
	params = ctx.parent.params
	st.cleanup(days=params.get('days'))
