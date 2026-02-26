import pytest
from unittest.mock import AsyncMock

from services.ctf_execution import CTFExecutionService


class _TxHash:
    def __init__(self, value: str) -> None:
        self._value = value

    def hex(self) -> str:
        return self._value


class _Receipt:
    status = 1


class _Signer:
    def __init__(self, txs: list[dict]) -> None:
        self._txs = txs

    def sign_transaction(self, tx: dict, _private_key: str):
        self._txs.append(dict(tx))
        return type("Signed", (), {"raw_transaction": b"rawtx"})()


class _EthEOAStub:
    def __init__(self, nonce: int, gas_price: int) -> None:
        self._nonce = nonce
        self.gas_price = gas_price
        self.account = _Signer([])
        self.nonce_calls: list[tuple[str, str | None]] = []

    def get_transaction_count(self, address: str, block_identifier: str | None = None) -> int:
        self.nonce_calls.append((address, block_identifier))
        return self._nonce

    def send_raw_transaction(self, _raw: bytes):
        return _TxHash("0xdeadbeef")

    def wait_for_transaction_receipt(self, _tx_hash, _timeout: int):
        return _Receipt()


class _Web3EOAStub:
    def __init__(self, nonce: int = 11, gas_price: int = 100):
        self.eth = _EthEOAStub(nonce=nonce, gas_price=gas_price)


class _CallResult:
    def __init__(self, value):
        self._value = value

    def call(self):
        return self._value


class _SafeExecBuilder:
    def __init__(self, built_txs: list[dict]) -> None:
        self._built_txs = built_txs

    def build_transaction(self, tx: dict) -> dict:
        built = dict(tx)
        self._built_txs.append(built)
        return built


class _SafeFunctionsStub:
    def __init__(self, built_txs: list[dict]) -> None:
        self._built_txs = built_txs

    def nonce(self):
        return _CallResult(7)

    def getTransactionHash(self, *_args):
        return _CallResult(b"\x11" * 32)

    def execTransaction(self, *_args):
        return _SafeExecBuilder(self._built_txs)


class _SafeContractStub:
    def __init__(self, built_txs: list[dict]) -> None:
        self.functions = _SafeFunctionsStub(built_txs)


class _EthSafeStub(_EthEOAStub):
    def __init__(self, nonce: int, gas_price: int, built_txs: list[dict]) -> None:
        super().__init__(nonce=nonce, gas_price=gas_price)
        self._built_txs = built_txs

    def contract(self, address: str, abi):
        _ = address, abi
        return _SafeContractStub(self._built_txs)


class _Web3SafeStub:
    def __init__(self, nonce: int = 13, gas_price: int = 120):
        self._built_txs: list[dict] = []
        self.eth = _EthSafeStub(nonce=nonce, gas_price=gas_price, built_txs=self._built_txs)


@pytest.mark.asyncio
async def test_send_eoa_call_uses_pending_nonce():
    service = CTFExecutionService()
    w3 = _Web3EOAStub(nonce=11, gas_price=100)

    tx_hash = await service._send_eoa_call(
        w3=w3,
        from_address="0xsender",
        private_key="0xabc123",
        to_address="0xcontract",
        data=b"\x01\x02",
        gas_limit=21000,
    )

    assert tx_hash == "0xdeadbeef"
    assert ("0xsender", "pending") in w3.eth.nonce_calls
    assert w3.eth.account._txs
    assert w3.eth.account._txs[0]["nonce"] == 11


@pytest.mark.asyncio
async def test_send_safe_call_uses_pending_owner_nonce(monkeypatch):
    service = CTFExecutionService()
    w3 = _Web3SafeStub(nonce=13, gas_price=120)
    monkeypatch.setattr(service, "_safe_signature", AsyncMock(return_value=b"sig"))

    tx_hash = await service._send_safe_call(
        w3=w3,
        safe_address="0xsafe",
        owner_eoa="0xowner",
        private_key="0xabc123",
        to_address="0xcontract",
        data=b"\x03\x04",
        gas_limit=250000,
    )

    assert tx_hash == "0xdeadbeef"
    assert ("0xowner", "pending") in w3.eth.nonce_calls
    assert w3._built_txs
    assert w3._built_txs[0]["nonce"] == 13
