"""Module containing command masking functionality."""
from typing import Any, Dict, Iterator, List, Union

import click

MASK = "*****"


def _recurse_cli(
    cli_element: Union[click.Command, click.Group, click.CommandCollection],
    ctx: click.Context,
    io_dict: Dict[str, Any],
    get_help: bool = False,
) -> None:
    """
    Code copied over from kedro.tools.cli to maintain backwards compatibility
    with previous versions of kedro (<0.17.5).

    Recursive function that checks the type of the command (key) and decides:
    1. In case of `click.Group` or `click.CommandCollection` (aggregate commands),
        the function collects the name and recurses one layer deeper
        for each sub-command.
    2. In case of `click.Command`, the terminus command has been reached. The function
        collects the name, parameters and args, flattens them and saves them as
        dictionary keys.
    Args:
        cli_element: CLI Collection as input for recursion, typically `KedroCLI`.
        ctx: Click Context, created by the wrapper function.
        io_dict: Input-output dictionary, mutated during the recursion.
        get_help: Boolean fork - allows either:
            raw structure - nested dictionary until final value of `None`
            help structure - nested dictionary where leaves are `--help` cmd output

    Returns:
        None (underlying `io_dict` is mutated by the recursion)
    """
    if isinstance(cli_element, (click.Group, click.CommandCollection)):
        element_name = cli_element.name or "kedro"
        io_dict[element_name] = {}
        for command_name in cli_element.list_commands(ctx):
            _recurse_cli(  # type: ignore
                cli_element.get_command(ctx, command_name),
                ctx,
                io_dict[element_name],
                get_help,
            )
        if not get_help:
            nested_parameter_list = [
                option.opts for option in cli_element.get_params(ctx)
            ]
            for item in (item for sublist in nested_parameter_list for item in sublist):
                if item not in io_dict[element_name]:
                    io_dict[element_name][item] = None

    elif isinstance(cli_element, click.Command):
        if get_help:  # gets formatted CLI help incl params for printing
            io_dict[cli_element.name] = cli_element.get_help(ctx)
        else:  # gets params for structure purposes
            nested_parameter_list = [
                option.opts for option in cli_element.get_params(ctx)
            ]
            io_dict[cli_element.name] = dict.fromkeys(
                [item for sublist in nested_parameter_list for item in sublist], None
            )


def _get_cli_structure(
    cli_obj: Union[click.Command, click.Group, click.CommandCollection],
    get_help: bool = False,
) -> Dict[str, Any]:
    """Code copied over from kedro.tools.cli to maintain backwards compatibility
    with previous versions of kedro (<0.17.5).
    Convenience wrapper function for `_recurse_cli` to work within
    `click.Context` and return a `dict`.
    """
    output: Dict[str, Any] = {}
    with click.Context(cli_obj) as ctx:  # type: ignore
        _recurse_cli(cli_obj, ctx, output, get_help)
    return output


def _mask_kedro_cli(cli_struct: Dict[str, Any], command_args: List[str]) -> List[str]:
    """Takes a dynamic vocabulary (based on `KedroCLI`) and returns
    a masked CLI input"""
    output = []

    # Preserve the initial part of the command until parameters sections begin
    arg_index = 0
    current_CLI = cli_struct.get("kedro", {})
    while (
        arg_index < len(command_args)
        and not command_args[arg_index].startswith("-")
        and command_args[arg_index] in current_CLI
    ):
        output.append(command_args[arg_index])
        current_CLI = current_CLI[command_args[arg_index]]
        arg_index += 1

    # Mask everything except parameter keywords
    for arg in command_args[arg_index:]:
        if arg.startswith("-"):
            if "=" in arg:
                arg_left = arg.split("=")[0]
                if arg_left in current_CLI:
                    output.append(arg_left)
                output.append(MASK)
            elif arg in current_CLI:
                output.append(arg)
            else:
                output.append(MASK)
        else:
            output.append(MASK)

    return output


def _recursive_items(dictionary: Dict[Any, Any]) -> Iterator[Any]:
    for key, value in dictionary.items():
        if isinstance(value, dict):
            yield key
            yield from _recursive_items(value)
        else:
            yield key
            yield value
