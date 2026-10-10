"""Instruction builders adapted from SolHouse Signal claimsol-tx (MIT).
Source commit: c8e9ba7afdb7800eda9afca2338a5e6d3e1a6440
Copyright (c) 2026 SolHouse Signal. See third_party/claimsol/LICENSE.
Only empty-account closure and checked token burn are used by SolBack.
"""
from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey


def ix_close(program, account, owner):
    """SPL CloseAccount (9): the rent goes back to `owner`, who must sign."""
    return Instruction(program, bytes([9]), [AccountMeta(account, False, True),
                                             AccountMeta(owner, False, True),
                                             AccountMeta(owner, True, False)])

def ix_burn(program, account, mint, owner, amount, decimals):
    """SPL BurnChecked (15) of the user's own remaining balance (only when the user asks)."""
    data = bytes([15]) + int(amount).to_bytes(8, "little") + bytes([decimals])
    return Instruction(program, data, [AccountMeta(account, False, True),
                                       AccountMeta(Pubkey.from_string(mint), False, True),
                                       AccountMeta(owner, True, False)])
