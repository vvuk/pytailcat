import json
from contextlib import ExitStack

import pytest

import pytailcat as tc
from pytailcat._ffi import Token, native


@pytest.mark.parametrize("network", [1, 2], ids=["tcp", "udp"])
def test_no_cancel_supports_native_peer_and_stream_calls(relay, network):
    lib = native()
    no_cancel = lib.lib.TC_NO_CANCEL
    assert no_cancel == 0

    with ExitStack() as resources:

        def own(name, *args):
            handle = lib.handle(name, *args)
            resources.callback(lib.close, handle)
            return handle

        def write(connection, data):
            count = lib.ffi.new("size_t *")
            lib.invoke("tc_conn_write", connection, no_cancel, data, len(data), count)
            assert count[0] == len(data)

        def read(connection, *, eof=False):
            buffer = lib.ffi.new("char[]", 128)
            count = lib.ffi.new("size_t *")
            code = lib.invoke(
                "tc_conn_read",
                connection,
                no_cancel,
                buffer,
                128,
                count,
                allow_eof=eof,
            )
            assert code == (lib.lib.TC_EOF if eof else lib.lib.TC_OK)
            return bytes(lib.ffi.buffer(buffer, count[0]))

        server = own("tc_server_new", json.dumps({"region": relay["region"]}).encode())
        lib.invoke("tc_server_start", server, no_cancel)
        listener = own("tc_server_listen", server, no_cancel, 0, network)
        port = int(
            lib.json("tc_info", listener, no_cancel)["local_address"].rsplit(":", 1)[1]
        )
        address = lib.json("tc_info", server, no_cancel)["address"]
        assert (
            lib.text("tc_address_resolve", no_cancel, address.encode(), lib.ffi.NULL)
            == address
        )
        client = own("tc_client_new", json.dumps({"address": address}).encode())
        public_key = lib.json("tc_info", client, no_cancel)["public_key"]
        lib.invoke("tc_server_allow_client", server, no_cancel, public_key.encode())
        outgoing = own("tc_client_dial", client, no_cancel, port, network)
        # UDP acceptance requires the first datagram to arrive.
        write(outgoing, b"hello")
        incoming = own("tc_listener_accept", listener, no_cancel)
        assert read(incoming) == b"hello"
        write(incoming, b"world")
        assert read(outgoing) == b"world"
        if network == lib.lib.TC_TCP:
            lib.invoke("tc_conn_close_write", outgoing, no_cancel)
            assert read(incoming, eof=True) == b""


def test_no_cancel_is_not_a_cancellable_or_closeable_resource():
    lib = native()
    for function in ("tc_token_cancel", "tc_close"):
        with pytest.raises(tc.ClosedError):
            lib.invoke(function, lib.lib.TC_NO_CANCEL)
    with pytest.raises(tc.ClosedError):
        lib.json("tc_info", lib.lib.TC_NO_CANCEL, lib.lib.TC_NO_CANCEL)


@pytest.mark.parametrize("disco", [False, True], ids=["relay", "disco"])
def test_native_ping_returns_milliseconds_or_discovery_json(relay, disco):
    lib = native()
    with tc.Client(relay["address"]) as client, Token(10) as token:
        if disco:
            out = lib.ffi.new("char **")
            lib.invoke("tc_client_disco_ping", client._handle, token.handle, out)
            try:
                result = json.loads(lib.ffi.string(out[0]))
                assert result["latency"] >= 0
                assert isinstance(result["endpoint"], str)
                assert isinstance(result["derp_region_id"], int)
                assert isinstance(result["derp_region_code"], str)
            finally:
                lib.lib.tc_free(out[0])
        else:
            ping_ms = lib.ffi.new("int32_t *", -1)
            lib.invoke("tc_client_ping", client._handle, token.handle, ping_ms)
            assert 0 <= ping_ms[0] < 10_000


@pytest.mark.parametrize("function", ["tc_client_ping", "tc_client_disco_ping"])
def test_native_ping_requires_output_storage(function):
    lib = native()
    with pytest.raises(tc.InvalidArgumentError):
        lib.invoke(function, 0, lib.lib.TC_NO_CANCEL, lib.ffi.NULL)


@pytest.mark.parametrize("function", ["tc_client_ping", "tc_client_disco_ping"])
@pytest.mark.parametrize("failure", ["closed", "wrong_type", "expired", "cancelled"])
def test_native_ping_clears_output_on_failure(relay, function, failure):
    lib = native()
    with (
        tc.Client(relay["address"]) as client,
        Token(0 if failure == "expired" else 10) as token,
    ):
        handle = client._handle
        if failure == "closed":
            client.close()
            error = tc.ClosedError
        elif failure == "wrong_type":
            handle = token.handle
            error = tc.InvalidArgumentError
        elif failure == "expired":
            error = tc.TailcatTimeout
        else:
            token.cancel()
            error = tc.CancelledError

        if function == "tc_client_ping":
            out = lib.ffi.new("int32_t *", -1)
            cleared = 0
        else:
            previous = lib.ffi.new("char[]", b"previous result")
            out = lib.ffi.new("char **", previous)
            cleared = lib.ffi.NULL

        with pytest.raises(error):
            lib.invoke(function, handle, token.handle, out)
        assert out[0] == cleared
