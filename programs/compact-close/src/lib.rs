//! Only batched standard CloseAccount; burn and fees are separate outer instructions.
#![allow(unexpected_cfgs)]
use solana_program::{account_info::AccountInfo,entrypoint,entrypoint::ProgramResult,
 instruction::{AccountMeta,Instruction},program::invoke,program_error::ProgramError,
 pubkey,pubkey::Pubkey};
entrypoint!(process_instruction);
const KEG:Pubkey=pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const T22:Pubkey=pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
pub fn process_instruction(_id:&Pubkey,a:&[AccountInfo],data:&[u8])->ProgramResult{
 if a.len()<4||a.len()>59||!data.is_empty(){return Err(ProgramError::InvalidInstructionData)}
 let owner=&a[0];let keg=&a[1];let t22=&a[2];
 if !owner.is_signer{return Err(ProgramError::MissingRequiredSignature)}
 if !owner.is_writable{return Err(ProgramError::InvalidAccountData)}
 if *keg.key!=KEG||*t22.key!=T22||!keg.executable||!t22.executable{return Err(ProgramError::IncorrectProgramId)}
 for (i,account) in a[3..].iter().enumerate(){
  if !account.is_writable||account.key==owner.key||a[3..3+i].iter().any(|prev|prev.key==account.key){return Err(ProgramError::InvalidAccountData)}
  let tp=if *account.owner==KEG{keg}else if *account.owner==T22{t22}else{return Err(ProgramError::IllegalOwner)};
  invoke(&Instruction{program_id:*tp.key,accounts:vec![AccountMeta::new(*account.key,false),AccountMeta::new(*owner.key,false),AccountMeta::new_readonly(*owner.key,true)],data:vec![9]},&[account.clone(),owner.clone(),owner.clone(),tp.clone()])?;
 }
 Ok(())
}
