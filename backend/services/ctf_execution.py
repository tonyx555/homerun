from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

from config import settings
from services.polymarket import polymarket_client
from services.live_execution_service import live_execution_service
from utils.converters import safe_float
from utils.logger import get_logger

logger = get_logger(__name__)

_USDC_DECIMALS = 6
_MAX_UINT256 = 2**256 - 1
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_MIN_APPROVAL_BUFFER_BASE = 10_000_000  # 10 USDC (6 decimals)
_NONCE_RACE_ERROR_MARKERS = (
    "replacement transaction underpriced",
    "transaction underpriced",
    "nonce too low",
)


@dataclass
class CTFExecutionResult:
    status: str
    action: str
    tx_hash: str | None
    error_message: str | None
    payload: dict[str, Any]


class CTFExecutionService:
    CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

    _CTF_ABI = [
        {
            "name": "splitPosition",
            "type": "function",
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "partition", "type": "uint256[]"},
                {"name": "amount", "type": "uint256"},
            ],
            "outputs": [],
            "stateMutability": "nonpayable",
        },
        {
            "name": "mergePositions",
            "type": "function",
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "partition", "type": "uint256[]"},
                {"name": "amount", "type": "uint256"},
            ],
            "outputs": [],
            "stateMutability": "nonpayable",
        },
        {
            "name": "redeemPositions",
            "type": "function",
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"},
            ],
            "outputs": [],
            "stateMutability": "nonpayable",
        },
        {
            "name": "balanceOf",
            "type": "function",
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"},
            ],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        },
        {
            "name": "payoutDenominator",
            "type": "function",
            "inputs": [{"name": "conditionId", "type": "bytes32"}],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        },
        {
            # CTF stores per-outcome-slot payout numerators after resolution.
            # For binary markets (slots 0/1), one will be 1 and the other 0
            # (or scaled so the pair sums to denominator). For scalar
            # markets the slots can carry fractional weights summing to
            # the denominator. Used by the redeemer's expected-payout
            # guard to skip $0-return redemptions that would only burn gas.
            "name": "payoutNumerators",
            "type": "function",
            "inputs": [
                {"name": "conditionId", "type": "bytes32"},
                {"name": "outcomeSlot", "type": "uint256"},
            ],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        },
        {
            "name": "isApprovedForAll",
            "type": "function",
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "operator", "type": "address"},
            ],
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "view",
        },
        {
            "name": "setApprovalForAll",
            "type": "function",
            "inputs": [
                {"name": "operator", "type": "address"},
                {"name": "approved", "type": "bool"},
            ],
            "outputs": [],
            "stateMutability": "nonpayable",
        },
    ]

    _ERC20_ABI = [
        {
            "name": "allowance",
            "type": "function",
            "inputs": [
                {"name": "owner", "type": "address"},
                {"name": "spender", "type": "address"},
            ],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        },
        {
            "name": "approve",
            "type": "function",
            "inputs": [
                {"name": "spender", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
        },
    ]

    _SAFE_ABI = [
        {
            "name": "nonce",
            "type": "function",
            "inputs": [],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        },
        {
            "name": "getTransactionHash",
            "type": "function",
            "inputs": [
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
                {"name": "operation", "type": "uint8"},
                {"name": "safeTxGas", "type": "uint256"},
                {"name": "baseGas", "type": "uint256"},
                {"name": "gasPrice", "type": "uint256"},
                {"name": "gasToken", "type": "address"},
                {"name": "refundReceiver", "type": "address"},
                {"name": "_nonce", "type": "uint256"},
            ],
            "outputs": [{"name": "", "type": "bytes32"}],
            "stateMutability": "view",
        },
        {
            "name": "execTransaction",
            "type": "function",
            "inputs": [
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
                {"name": "operation", "type": "uint8"},
                {"name": "safeTxGas", "type": "uint256"},
                {"name": "baseGas", "type": "uint256"},
                {"name": "gasPrice", "type": "uint256"},
                {"name": "gasToken", "type": "address"},
                {"name": "refundReceiver", "type": "address"},
                {"name": "signatures", "type": "bytes"},
            ],
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "payable",
        },
    ]

    def __init__(self) -> None:
        self._tx_lock = asyncio.Lock()

    def _rpc_candidates(self) -> list[str]:
        candidates: list[str] = []
        for raw in (
            settings.POLYGON_RPC_URL,
            "https://polygon-rpc.com",
            "https://rpc-mainnet.matic.quiknode.pro",
            "https://polygon.gateway.tenderly.co",
        ):
            url = str(raw or "").strip()
            if not url or url in candidates:
                continue
            candidates.append(url)
        return candidates

    async def _get_web3(self):
        from web3 import Web3

        last_error: Exception | None = None
        for rpc_url in self._rpc_candidates():
            try:
                candidate = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 12}))
                await asyncio.to_thread(lambda: candidate.eth.block_number)
                return candidate
            except Exception as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise RuntimeError(f"All Polygon RPC providers failed: {last_error}")
        raise RuntimeError("No Polygon RPC provider configured")

    async def _resolve_wallet_context(self) -> dict[str, str]:
        from eth_account import Account

        private_key, _, _, _, _ = await live_execution_service._resolve_polymarket_credentials()
        if not private_key:
            raise RuntimeError("Missing Polymarket private key for CTF execution")

        eoa = str(getattr(live_execution_service, "_eoa_address", "") or "").strip()
        if not eoa:
            eoa = Account.from_key(private_key).address

        execution_wallet = str(live_execution_service.get_execution_wallet_address() or "").strip()
        if not execution_wallet:
            execution_wallet = eoa

        return {
            "private_key": private_key,
            "eoa_address": eoa,
            "execution_wallet": execution_wallet,
        }

    async def _safe_signature(self, tx_hash_bytes: bytes, private_key: str) -> bytes:
        from eth_keys import keys

        def _sign_digest() -> bytes:
            normalized_key = str(private_key or "").strip()
            if normalized_key.startswith("0x"):
                normalized_key = normalized_key[2:]
            signer = keys.PrivateKey(bytes.fromhex(normalized_key))
            signature = signer.sign_msg_hash(tx_hash_bytes)
            v = int(signature.v) + 27
            return signature.r.to_bytes(32, "big") + signature.s.to_bytes(32, "big") + bytes([v])

        return await asyncio.to_thread(_sign_digest)

    async def _is_safe_wallet(self, w3, wallet_address: str) -> bool:
        try:
            checksum_wallet = w3.to_checksum_address(wallet_address)
            code = await asyncio.to_thread(lambda: w3.eth.get_code(checksum_wallet))
            if not code:
                return False
            safe = w3.eth.contract(address=checksum_wallet, abi=self._SAFE_ABI)
            await asyncio.to_thread(lambda: safe.functions.nonce().call())
            return True
        except Exception:
            return False

    async def _next_sender_nonce(self, w3, address: str) -> int:
        try:
            return int(await asyncio.to_thread(lambda: w3.eth.get_transaction_count(address, "pending")))
        except TypeError:
            return int(await asyncio.to_thread(lambda: w3.eth.get_transaction_count(address)))

    async def _send_eoa_call(
        self,
        *,
        w3,
        from_address: str,
        private_key: str,
        to_address: str,
        data: bytes,
        gas_limit: int,
    ) -> str:
        chain_id = int(getattr(settings, "CHAIN_ID", 137) or 137)

        async with self._tx_lock:
            nonce = await self._next_sender_nonce(w3, from_address)
            gas_price = await asyncio.to_thread(lambda: w3.eth.gas_price)
            tx = {
                "from": from_address,
                "to": to_address,
                "value": 0,
                "data": data,
                "nonce": nonce,
                "gas": int(gas_limit),
                "gasPrice": gas_price,
                "chainId": chain_id,
            }
            signed = await asyncio.to_thread(w3.eth.account.sign_transaction, tx, private_key)
            tx_hash = await asyncio.to_thread(w3.eth.send_raw_transaction, signed.raw_transaction)
            receipt = await asyncio.to_thread(w3.eth.wait_for_transaction_receipt, tx_hash, 120)

        if int(getattr(receipt, "status", 0) or 0) != 1:
            raise RuntimeError("On-chain transaction reverted")
        return tx_hash.hex()

    async def _send_safe_call(
        self,
        *,
        w3,
        safe_address: str,
        owner_eoa: str,
        private_key: str,
        to_address: str,
        data: bytes,
        gas_limit: int,
    ) -> str:
        chain_id = int(getattr(settings, "CHAIN_ID", 137) or 137)
        safe = w3.eth.contract(address=safe_address, abi=self._SAFE_ABI)

        async with self._tx_lock:
            safe_nonce = await asyncio.to_thread(lambda: safe.functions.nonce().call())
            safe_tx_hash = await asyncio.to_thread(
                lambda: safe.functions.getTransactionHash(
                    to_address,
                    0,
                    data,
                    0,
                    0,
                    0,
                    0,
                    _ZERO_ADDRESS,
                    _ZERO_ADDRESS,
                    safe_nonce,
                ).call()
            )
            signature = await self._safe_signature(safe_tx_hash, private_key)

            owner_nonce = await self._next_sender_nonce(w3, owner_eoa)
            gas_price = await asyncio.to_thread(lambda: w3.eth.gas_price)
            tx = safe.functions.execTransaction(
                to_address,
                0,
                data,
                0,
                0,
                0,
                0,
                _ZERO_ADDRESS,
                _ZERO_ADDRESS,
                signature,
            ).build_transaction(
                {
                    "from": owner_eoa,
                    "nonce": owner_nonce,
                    "gas": int(gas_limit),
                    "gasPrice": gas_price,
                    "chainId": chain_id,
                }
            )
            signed = await asyncio.to_thread(w3.eth.account.sign_transaction, tx, private_key)
            tx_hash = await asyncio.to_thread(w3.eth.send_raw_transaction, signed.raw_transaction)
            receipt = await asyncio.to_thread(w3.eth.wait_for_transaction_receipt, tx_hash, 120)

        if int(getattr(receipt, "status", 0) or 0) != 1:
            raise RuntimeError("Safe transaction reverted")
        return tx_hash.hex()

    async def _execute_contract_call(
        self,
        *,
        contract_address: str,
        data: bytes,
        gas_limit: int,
        action: str,
    ) -> CTFExecutionResult:
        try:
            wallet_ctx = await self._resolve_wallet_context()
            w3 = await self._get_web3()
            execution_wallet = w3.to_checksum_address(wallet_ctx["execution_wallet"])
            eoa = w3.to_checksum_address(wallet_ctx["eoa_address"])
            to_address = w3.to_checksum_address(contract_address)

            if await self._is_safe_wallet(w3, execution_wallet):
                tx_hash = await self._send_safe_call(
                    w3=w3,
                    safe_address=execution_wallet,
                    owner_eoa=eoa,
                    private_key=wallet_ctx["private_key"],
                    to_address=to_address,
                    data=data,
                    gas_limit=gas_limit,
                )
            else:
                tx_hash = await self._send_eoa_call(
                    w3=w3,
                    from_address=eoa,
                    private_key=wallet_ctx["private_key"],
                    to_address=to_address,
                    data=data,
                    gas_limit=gas_limit,
                )
            return CTFExecutionResult(
                status="executed",
                action=action,
                tx_hash=tx_hash,
                error_message=None,
                payload={
                    "execution_wallet": wallet_ctx["execution_wallet"],
                    "eoa_address": wallet_ctx["eoa_address"],
                    "contract_address": contract_address,
                },
            )
        except Exception as exc:
            error_message = str(exc)
            normalized_error = error_message.lower()
            if "insufficient funds for gas" in normalized_error:
                logger.warning(
                    "CTF contract call skipped due to insufficient native gas",
                    action=action,
                    error=error_message,
                )
            elif any(marker in normalized_error for marker in _NONCE_RACE_ERROR_MARKERS):
                logger.warning(
                    "CTF contract call deferred due to nonce/gas pricing race",
                    action=action,
                    error=error_message,
                )
            else:
                logger.error("CTF contract call failed", action=action, exc_info=exc)
            return CTFExecutionResult(
                status="failed",
                action=action,
                tx_hash=None,
                error_message=error_message,
                payload={"contract_address": contract_address},
            )

    def _normalize_condition_id(self, raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if not text.startswith("0x"):
            text = f"0x{text}"
        if len(text) != 66:
            raise ValueError("condition_id must be a 32-byte hex string")
        int(text[2:], 16)
        return text

    def _to_base_units(self, amount: float) -> int:
        normalized = max(0.0, float(amount))
        return int(round(normalized * (10**_USDC_DECIMALS)))

    def _parse_token_id_uint256(self, token_id: Any) -> Optional[int]:
        text = str(token_id or "").strip().lower()
        if not text:
            return None
        try:
            if text.startswith("0x"):
                return int(text, 16)
            return int(text)
        except Exception:
            return None

    async def ensure_usdc_approval(self, *, min_amount_base_units: int = 0) -> CTFExecutionResult:
        try:
            wallet_ctx = await self._resolve_wallet_context()
            w3 = await self._get_web3()
            owner = w3.to_checksum_address(wallet_ctx["execution_wallet"])
            usdc_address = w3.to_checksum_address(self.USDC_ADDRESS)
            ctf_address = w3.to_checksum_address(self.CTF_ADDRESS)
            usdc = w3.eth.contract(address=usdc_address, abi=self._ERC20_ABI)
            allowance = await asyncio.to_thread(lambda: usdc.functions.allowance(owner, ctf_address).call())
            required = max(int(min_amount_base_units), _MIN_APPROVAL_BUFFER_BASE)
            if int(allowance or 0) >= required:
                return CTFExecutionResult(
                    status="executed",
                    action="approve_usdc",
                    tx_hash=None,
                    error_message=None,
                    payload={"allowance": int(allowance), "required": required, "already_approved": True},
                )

            data = usdc.functions.approve(ctf_address, _MAX_UINT256)._encode_transaction_data()
            result = await self._execute_contract_call(
                contract_address=self.USDC_ADDRESS,
                data=data,
                gas_limit=140_000,
                action="approve_usdc",
            )
            if result.status == "executed":
                result.payload.update({"required": required, "already_approved": False})
            return result
        except Exception as exc:
            logger.error("USDC approval check failed", exc_info=exc)
            return CTFExecutionResult(
                status="failed",
                action="approve_usdc",
                tx_hash=None,
                error_message=str(exc),
                payload={},
            )

    async def ensure_exchange_approval(self) -> CTFExecutionResult:
        try:
            wallet_ctx = await self._resolve_wallet_context()
            w3 = await self._get_web3()
            owner = w3.to_checksum_address(wallet_ctx["execution_wallet"])
            ctf_address = w3.to_checksum_address(self.CTF_ADDRESS)
            exchange_address = w3.to_checksum_address(self.CTF_EXCHANGE)
            ctf = w3.eth.contract(address=ctf_address, abi=self._CTF_ABI)
            approved = await asyncio.to_thread(lambda: ctf.functions.isApprovedForAll(owner, exchange_address).call())
            if bool(approved):
                return CTFExecutionResult(
                    status="executed",
                    action="approve_exchange",
                    tx_hash=None,
                    error_message=None,
                    payload={"already_approved": True},
                )

            data = ctf.functions.setApprovalForAll(exchange_address, True)._encode_transaction_data()
            result = await self._execute_contract_call(
                contract_address=self.CTF_ADDRESS,
                data=data,
                gas_limit=170_000,
                action="approve_exchange",
            )
            if result.status == "executed":
                result.payload.update({"already_approved": False})
            return result
        except Exception as exc:
            logger.error("Exchange approval check failed", exc_info=exc)
            return CTFExecutionResult(
                status="failed",
                action="approve_exchange",
                tx_hash=None,
                error_message=str(exc),
                payload={},
            )

    async def get_native_gas_affordability(self, *, gas_limit: int) -> dict[str, Any]:
        try:
            wallet_ctx = await self._resolve_wallet_context()
            w3 = await self._get_web3()
            eoa_address = w3.to_checksum_address(wallet_ctx["eoa_address"])
            balance_wei = int(await asyncio.to_thread(lambda: w3.eth.get_balance(eoa_address)) or 0)
            gas_price_wei = int(await asyncio.to_thread(lambda: w3.eth.gas_price) or 0)
            required_wei = max(0, int(gas_limit)) * max(0, gas_price_wei)
            return {
                "affordable": balance_wei >= required_wei,
                "balance_wei": balance_wei,
                "required_wei": required_wei,
                "gas_price_wei": gas_price_wei,
                "wallet_address": eoa_address,
            }
        except Exception as exc:
            return {
                "affordable": False,
                "balance_wei": 0,
                "required_wei": 0,
                "gas_price_wei": 0,
                "wallet_address": "",
                "error": str(exc),
            }

    async def split_position(self, *, condition_id: str, amount_usd: float) -> CTFExecutionResult:
        normalized_condition_id = self._normalize_condition_id(condition_id)
        amount = max(0.0, safe_float(amount_usd, 0.0) or 0.0)
        if amount <= 0.0:
            return CTFExecutionResult(
                status="failed",
                action="split",
                tx_hash=None,
                error_message="amount_usd must be greater than zero",
                payload={"condition_id": normalized_condition_id},
            )

        amount_base = self._to_base_units(amount)
        approval = await self.ensure_usdc_approval(min_amount_base_units=amount_base)
        if approval.status != "executed":
            return approval

        exchange_approval = await self.ensure_exchange_approval()
        if exchange_approval.status != "executed":
            return exchange_approval

        w3 = await self._get_web3()
        ctf = w3.eth.contract(address=w3.to_checksum_address(self.CTF_ADDRESS), abi=self._CTF_ABI)
        data = ctf.functions.splitPosition(
            self.USDC_ADDRESS,
            "0x" + ("00" * 32),
            normalized_condition_id,
            [1, 2],
            amount_base,
        )._encode_transaction_data()
        result = await self._execute_contract_call(
            contract_address=self.CTF_ADDRESS,
            data=data,
            gas_limit=320_000,
            action="split",
        )
        result.payload.update(
            {
                "condition_id": normalized_condition_id,
                "amount_usd": amount,
                "amount_base_units": amount_base,
                "shares_per_side": amount,
            }
        )
        return result

    async def merge_positions(self, *, condition_id: str, shares_per_side: float) -> CTFExecutionResult:
        normalized_condition_id = self._normalize_condition_id(condition_id)
        shares = max(0.0, safe_float(shares_per_side, 0.0) or 0.0)
        if shares <= 0.0:
            return CTFExecutionResult(
                status="failed",
                action="merge",
                tx_hash=None,
                error_message="shares_per_side must be greater than zero",
                payload={"condition_id": normalized_condition_id},
            )

        amount_base = self._to_base_units(shares)
        w3 = await self._get_web3()
        ctf = w3.eth.contract(address=w3.to_checksum_address(self.CTF_ADDRESS), abi=self._CTF_ABI)
        data = ctf.functions.mergePositions(
            self.USDC_ADDRESS,
            "0x" + ("00" * 32),
            normalized_condition_id,
            [1, 2],
            amount_base,
        )._encode_transaction_data()
        result = await self._execute_contract_call(
            contract_address=self.CTF_ADDRESS,
            data=data,
            gas_limit=280_000,
            action="merge",
        )
        result.payload.update(
            {
                "condition_id": normalized_condition_id,
                "shares_per_side": shares,
                "amount_base_units": amount_base,
            }
        )
        return result

    async def redeem_positions(
        self,
        *,
        condition_id: str,
        index_sets: list[int] | None = None,
    ) -> CTFExecutionResult:
        normalized_condition_id = self._normalize_condition_id(condition_id)
        normalized_sets = [int(value) for value in (index_sets or [1, 2]) if int(value) > 0]
        if not normalized_sets:
            normalized_sets = [1, 2]

        w3 = await self._get_web3()
        ctf = w3.eth.contract(address=w3.to_checksum_address(self.CTF_ADDRESS), abi=self._CTF_ABI)
        data = ctf.functions.redeemPositions(
            self.USDC_ADDRESS,
            "0x" + ("00" * 32),
            normalized_condition_id,
            normalized_sets,
        )._encode_transaction_data()
        result = await self._execute_contract_call(
            contract_address=self.CTF_ADDRESS,
            data=data,
            gas_limit=260_000,
            action="redeem",
        )
        result.payload.update(
            {
                "condition_id": normalized_condition_id,
                "index_sets": normalized_sets,
            }
        )
        return result

    @staticmethod
    def compute_condition_payout_breakdown(
        *,
        denominator: int,
        outcome_balances: dict[int, float],
        outcome_numerators: dict[int, int],
    ) -> dict[str, float]:
        """Pure-function payout math used by the redeemer guard.

        Given the on-chain ``denominator`` and per-outcome-slot
        ``numerators`` from ``payoutNumerators(conditionId, slot)``,
        plus the wallet's per-slot ``balances`` (already in human shares,
        NOT raw uint256), returns a breakdown of expected USDC payout if
        we redeem now.

        Math (binary or scalar): for each held outcome slot,
        ``payout_per_share = numerator[slot] / denominator``, and the
        condition payout is the sum of ``balance[slot] * payout_per_share``
        across every slot the wallet holds.

        For a fully-losing binary position, the held slot has
        ``numerator = 0``, so ``expected_payout_usd = 0`` and the redeemer
        will skip unless the operator force-redeems. Extracted to a static
        method so the math has unit tests independent of web3/RPC mocks.
        """
        denominator_f = float(denominator or 0)
        if denominator_f <= 0:
            return {
                "expected_payout_usd": 0.0,
                "total_shares": float(sum(outcome_balances.values()) or 0.0),
                "winning_shares": 0.0,
                "losing_shares": float(sum(outcome_balances.values()) or 0.0),
            }
        expected_payout_usd = 0.0
        winning_shares = 0.0
        losing_shares = 0.0
        for slot, balance in outcome_balances.items():
            numerator = float(outcome_numerators.get(int(slot), 0) or 0)
            if numerator > 0:
                expected_payout_usd += float(balance) * (numerator / denominator_f)
                winning_shares += float(balance)
            else:
                losing_shares += float(balance)
        return {
            "expected_payout_usd": expected_payout_usd,
            "total_shares": winning_shares + losing_shares,
            "winning_shares": winning_shares,
            "losing_shares": losing_shares,
        }

    async def fetch_position_chain_status(
        self,
        *,
        wallet_address: str,
        token_id: str,
        condition_id: str,
        outcome_index: int,
    ) -> dict[str, Any]:
        """Read-only on-chain truth for a single conditional-token holding.

        Returns a structured dict with the wallet's CTF balance, the
        market's resolution state, and the deterministic redeemable
        payout if redemption fired right now.

        This is the institutional-grade truth primitive: it talks to
        Polygon mainnet directly (NOT Polymarket's data API), so the
        answer is exactly what an auditor would compute from on-chain
        evidence.  Callers (the stuck-position monitor, the
        manual-writeoff API, the verifier's resolution path) should
        prefer this over polymarket_client when they need certainty
        rather than UI-grade freshness.

        Returned shape::

          {
            "wallet_address":        str (lowercase),
            "token_id":              str,
            "condition_id":          str (0x-prefixed bytes32),
            "outcome_index":         int,
            "wallet_balance_shares": float,   # ERC1155 balanceOf / 1e6
            "market_resolved":       bool,    # payoutDenominator > 0
            "payout_denominator":    int,
            "payout_numerator":      int,     # for THIS outcome slot
            "winning":               bool|None,  # numerator > 0
            "expected_payout_usdc":  float,   # balance * num/den
            "block_number":          int,     # for audit traceability
            "error":                 str|None,
          }

        ALL numerical values come from chain reads, NOT data-API.  No
        caching at this layer — the caller is responsible for not
        spamming this method.
        """
        normalized_wallet = (wallet_address or "").strip().lower()
        normalized_token = (token_id or "").strip()
        normalized_condition = (condition_id or "").strip().lower()
        if not normalized_wallet or not normalized_token or not normalized_condition:
            return {
                "wallet_address": normalized_wallet,
                "token_id": normalized_token,
                "condition_id": normalized_condition,
                "outcome_index": int(outcome_index),
                "wallet_balance_shares": 0.0,
                "market_resolved": False,
                "payout_denominator": 0,
                "payout_numerator": 0,
                "winning": None,
                "expected_payout_usdc": 0.0,
                "block_number": 0,
                "error": "missing_required_input",
            }
        if not normalized_condition.startswith("0x") or len(normalized_condition) != 66:
            return {
                "wallet_address": normalized_wallet,
                "token_id": normalized_token,
                "condition_id": normalized_condition,
                "outcome_index": int(outcome_index),
                "wallet_balance_shares": 0.0,
                "market_resolved": False,
                "payout_denominator": 0,
                "payout_numerator": 0,
                "winning": None,
                "expected_payout_usdc": 0.0,
                "block_number": 0,
                "error": "invalid_condition_id_format",
            }
        try:
            token_id_uint = int(normalized_token)
        except (TypeError, ValueError):
            return {
                "wallet_address": normalized_wallet,
                "token_id": normalized_token,
                "condition_id": normalized_condition,
                "outcome_index": int(outcome_index),
                "wallet_balance_shares": 0.0,
                "market_resolved": False,
                "payout_denominator": 0,
                "payout_numerator": 0,
                "winning": None,
                "expected_payout_usdc": 0.0,
                "block_number": 0,
                "error": "invalid_token_id_format",
            }

        try:
            w3 = await self._get_web3()
        except Exception as exc:
            return {
                "wallet_address": normalized_wallet,
                "token_id": normalized_token,
                "condition_id": normalized_condition,
                "outcome_index": int(outcome_index),
                "wallet_balance_shares": 0.0,
                "market_resolved": False,
                "payout_denominator": 0,
                "payout_numerator": 0,
                "winning": None,
                "expected_payout_usdc": 0.0,
                "block_number": 0,
                "error": f"rpc_unavailable:{exc}",
            }

        ctf = w3.eth.contract(
            address=w3.to_checksum_address(self.CTF_ADDRESS),
            abi=self._CTF_ABI,
        )
        checksum_wallet = w3.to_checksum_address(normalized_wallet)

        try:
            block_number, raw_balance, denominator, numerator = await asyncio.gather(
                asyncio.to_thread(lambda: int(w3.eth.block_number)),
                asyncio.to_thread(
                    lambda: int(ctf.functions.balanceOf(checksum_wallet, token_id_uint).call())
                ),
                asyncio.to_thread(
                    lambda: int(ctf.functions.payoutDenominator(normalized_condition).call())
                ),
                asyncio.to_thread(
                    lambda: int(
                        ctf.functions.payoutNumerators(
                            normalized_condition, int(outcome_index)
                        ).call()
                    )
                ),
            )
        except Exception as exc:
            return {
                "wallet_address": normalized_wallet,
                "token_id": normalized_token,
                "condition_id": normalized_condition,
                "outcome_index": int(outcome_index),
                "wallet_balance_shares": 0.0,
                "market_resolved": False,
                "payout_denominator": 0,
                "payout_numerator": 0,
                "winning": None,
                "expected_payout_usdc": 0.0,
                "block_number": 0,
                "error": f"rpc_call_failed:{type(exc).__name__}:{exc}",
            }

        balance_shares = float(raw_balance) / float(10**_USDC_DECIMALS)
        market_resolved = denominator > 0
        winning: bool | None
        if not market_resolved:
            winning = None
            expected_payout = 0.0
        else:
            winning = numerator > 0
            if denominator > 0 and balance_shares > 0:
                expected_payout = balance_shares * (float(numerator) / float(denominator))
            else:
                expected_payout = 0.0

        return {
            "wallet_address": normalized_wallet,
            "token_id": normalized_token,
            "condition_id": normalized_condition,
            "outcome_index": int(outcome_index),
            "wallet_balance_shares": balance_shares,
            "market_resolved": market_resolved,
            "payout_denominator": denominator,
            "payout_numerator": numerator,
            "winning": winning,
            "expected_payout_usdc": expected_payout,
            "block_number": block_number,
            "error": None,
        }

    async def _gas_price_gwei(self, w3) -> float:
        # Lightweight TTL cache so a busy redeemer doesn't hammer the RPC.
        # Class-level so cache survives across calls within the process.
        import time as _time

        cache = getattr(self, "_gas_price_cache", None)
        ttl_seconds = 30.0
        now = _time.monotonic()
        if isinstance(cache, tuple) and (now - cache[0]) < ttl_seconds:
            return float(cache[1])
        try:
            wei = await asyncio.to_thread(lambda: w3.eth.gas_price)
        except Exception:
            return 0.0
        gwei = float(wei or 0) / 1e9
        self._gas_price_cache = (now, gwei)
        return gwei

    async def redeem_resolved_wallet_positions(
        self,
        *,
        wallet_address: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        execution_wallet = (
            str(wallet_address or live_execution_service.get_execution_wallet_address() or "").strip().lower()
        )
        if not execution_wallet:
            return {
                "wallet_address": "",
                "positions_scanned": 0,
                "conditions_checked": 0,
                "resolved_conditions": 0,
                "redeemable_value_usd": 0.0,
                "redeemed": 0,
                "skipped_low_payout": 0,
                "skipped_high_gas": 0,
                "failed": 0,
                "dry_run": bool(dry_run),
                "errors": ["missing_execution_wallet"],
            }

        positions = await polymarket_client.get_wallet_positions(execution_wallet)
        if not positions:
            return {
                "wallet_address": execution_wallet,
                "positions_scanned": 0,
                "conditions_checked": 0,
                "resolved_conditions": 0,
                "redeemable_value_usd": 0.0,
                "redeemed": 0,
                "skipped_low_payout": 0,
                "skipped_high_gas": 0,
                "failed": 0,
                "dry_run": bool(dry_run),
                "errors": [],
            }

        # Group positions by conditionId, keeping per-token outcomeIndex so
        # the payout math knows which slot each token represents.
        @dataclass
        class _HeldOutcome:
            token_id_uint: int
            outcome_index: int

        grouped: dict[str, list[_HeldOutcome]] = {}
        for row in positions:
            condition_id_raw = row.get("conditionId") or row.get("condition_id") or row.get("market")
            token_id_raw = row.get("asset") or row.get("asset_id") or row.get("token_id") or row.get("tokenId")
            outcome_index_raw = row.get("outcomeIndex")
            try:
                condition_id = self._normalize_condition_id(condition_id_raw)
            except Exception:
                continue
            token_id_uint = self._parse_token_id_uint256(token_id_raw)
            if token_id_uint is None:
                continue
            try:
                outcome_index = int(outcome_index_raw) if outcome_index_raw is not None else 0
            except (TypeError, ValueError):
                outcome_index = 0
            grouped.setdefault(condition_id, []).append(
                _HeldOutcome(token_id_uint=token_id_uint, outcome_index=outcome_index)
            )

        if not grouped:
            return {
                "wallet_address": execution_wallet,
                "positions_scanned": len(positions),
                "conditions_checked": 0,
                "resolved_conditions": 0,
                "redeemable_value_usd": 0.0,
                "redeemed": 0,
                "skipped_low_payout": 0,
                "skipped_high_gas": 0,
                "failed": 0,
                "dry_run": bool(dry_run),
                "errors": [],
            }

        w3 = await self._get_web3()
        ctf = w3.eth.contract(address=w3.to_checksum_address(self.CTF_ADDRESS), abi=self._CTF_ABI)
        checksum_wallet = w3.to_checksum_address(execution_wallet)

        # Operator-tunable policy (config + DB-overridable; see
        # config.Settings + alembic 202604280001).
        min_payout_usd = float(getattr(settings, "REDEEMER_MIN_PAYOUT_USD", 0.10) or 0.0)
        max_gas_price_gwei = float(getattr(settings, "REDEEMER_MAX_GAS_PRICE_GWEI", 200.0) or 0.0)
        force_losers = bool(getattr(settings, "REDEEMER_FORCE_INCLUDING_LOSERS", False))

        # Snapshot gas price once per cycle (cached) — defers cleanup
        # to a cheaper window when network is hot, without aborting the
        # whole cycle (we can still redeem high-value winners).
        gas_price_gwei = await self._gas_price_gwei(w3) if max_gas_price_gwei > 0 else 0.0
        gas_too_hot = max_gas_price_gwei > 0 and gas_price_gwei > max_gas_price_gwei

        redeemed = 0
        skipped_low_payout = 0
        skipped_high_gas = 0
        failed = 0
        resolved = 0
        redeemable_value_usd = 0.0
        errors: list[str] = []

        for condition_id, holdings in grouped.items():
            try:
                denominator = await asyncio.to_thread(
                    lambda: ctf.functions.payoutDenominator(condition_id).call()
                )
                if int(denominator or 0) <= 0:
                    continue
                resolved += 1

                # Read per-outcome numerators only for the slots we hold —
                # avoids guessing the market's outcome count.
                slots = sorted({int(h.outcome_index) for h in holdings})
                outcome_numerators: dict[int, int] = {}
                for slot in slots:
                    try:
                        n = await asyncio.to_thread(
                            lambda s=slot: ctf.functions.payoutNumerators(condition_id, s).call()
                        )
                        outcome_numerators[slot] = int(n or 0)
                    except Exception as exc:
                        # Treat unreadable slot as zero-payout; we'd rather
                        # under-redeem than burn gas on a bad estimate.
                        outcome_numerators[slot] = 0
                        errors.append(f"numerator_read_failed:{condition_id}:slot={slot}:{exc}")

                # Per-slot balances (chain truth, not data-API mark).
                outcome_balances: dict[int, float] = {}
                for held in holdings:
                    raw_balance = await asyncio.to_thread(
                        lambda token=held.token_id_uint: ctf.functions.balanceOf(
                            checksum_wallet, token
                        ).call()
                    )
                    shares = float(raw_balance or 0) / (10**_USDC_DECIMALS)
                    outcome_balances[int(held.outcome_index)] = (
                        outcome_balances.get(int(held.outcome_index), 0.0) + shares
                    )

                breakdown = self.compute_condition_payout_breakdown(
                    denominator=int(denominator),
                    outcome_balances=outcome_balances,
                    outcome_numerators=outcome_numerators,
                )
                expected_payout_usd = breakdown["expected_payout_usd"]
                total_shares = breakdown["total_shares"]
                redeemable_value_usd += expected_payout_usd

                # Wallets without dust shouldn't be on the list; if all
                # balances are zero we already redeemed at some point.
                if total_shares <= 1e-6:
                    continue

                # Decision matrix
                will_redeem = False
                skip_reason: str | None = None
                if expected_payout_usd >= min_payout_usd:
                    if gas_too_hot:
                        will_redeem = False
                        skip_reason = (
                            f"gas_price_too_high gwei={gas_price_gwei:.2f} ceiling={max_gas_price_gwei:.2f}"
                        )
                        skipped_high_gas += 1
                    else:
                        will_redeem = True
                elif force_losers:
                    if gas_too_hot:
                        will_redeem = False
                        skip_reason = (
                            f"gas_price_too_high (force-losers cycle) gwei={gas_price_gwei:.2f} "
                            f"ceiling={max_gas_price_gwei:.2f}"
                        )
                        skipped_high_gas += 1
                    else:
                        will_redeem = True  # dust-cleanup mode, gas burn accepted
                else:
                    will_redeem = False
                    skip_reason = (
                        f"expected_payout_below_floor "
                        f"payout=${expected_payout_usd:.4f} floor=${min_payout_usd:.4f} "
                        f"shares_winning={breakdown['winning_shares']:.4f} "
                        f"shares_losing={breakdown['losing_shares']:.4f}"
                    )
                    skipped_low_payout += 1

                logger.info(
                    "redeemer.decision condition=%s payout_usd=%.4f total_shares=%.4f "
                    "winning_shares=%.4f losing_shares=%.4f redeem=%s reason=%s gas_gwei=%.2f dry_run=%s",
                    condition_id,
                    expected_payout_usd,
                    total_shares,
                    breakdown["winning_shares"],
                    breakdown["losing_shares"],
                    bool(will_redeem),
                    skip_reason or "above_floor",
                    gas_price_gwei,
                    bool(dry_run),
                )

                if not will_redeem or dry_run:
                    if dry_run and will_redeem:
                        # In dry-run we still want the "would have redeemed"
                        # counter so operators see what a requested run will do.
                        redeemed += 1
                    continue

                redeem_result = await self.redeem_positions(
                    condition_id=condition_id, index_sets=[1, 2]
                )
                if redeem_result.status == "executed":
                    redeemed += 1
                else:
                    failed += 1
                    errors.append(
                        str(redeem_result.error_message or f"redeem_failed:{condition_id}")
                    )
            except Exception as exc:
                failed += 1
                errors.append(str(exc))

        return {
            "wallet_address": execution_wallet,
            "positions_scanned": len(positions),
            "conditions_checked": len(grouped),
            "resolved_conditions": resolved,
            "redeemable_value_usd": round(redeemable_value_usd, 4),
            "redeemed": redeemed,
            "skipped_low_payout": skipped_low_payout,
            "skipped_high_gas": skipped_high_gas,
            "failed": failed,
            "gas_price_gwei": round(gas_price_gwei, 2),
            "policy": {
                "min_payout_usd": min_payout_usd,
                "max_gas_price_gwei": max_gas_price_gwei,
                "force_including_losers": force_losers,
            },
            "dry_run": bool(dry_run),
            "errors": errors,
        }


ctf_execution_service = CTFExecutionService()
