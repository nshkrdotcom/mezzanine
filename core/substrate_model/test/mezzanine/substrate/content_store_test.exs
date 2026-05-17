defmodule Mezzanine.Substrate.ContentStoreTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ContentStore
  alias Mezzanine.Substrate.PayloadEnvelope
  alias Mezzanine.Substrate.ResultEnvelope

  test "validates inline payloads and content-addressed payload refs" do
    assert {:ok, inline} =
             PayloadEnvelope.new(%{
               payload_ref: "payload://tenant-a/inline-a",
               storage_mode: :inline,
               schema_ref: "schema://payload",
               redaction_ref: "redaction://standard",
               data: %{title: "Document"}
             })

    assert inline.storage_mode == :inline

    assert {:error, {:missing_content_field, :content_hash}} =
             PayloadEnvelope.new(%{
               payload_ref: "payload://tenant-a/content-a",
               storage_mode: :content_addressed,
               schema_ref: "schema://payload",
               redaction_ref: "redaction://standard",
               content_ref: "content://tenant-a/hash-a",
               byte_size: 12,
               store_ref: "store://local"
             })
  end

  test "validates result envelope storage modes" do
    assert {:ok, result} =
             ResultEnvelope.new(%{
               result_ref: "result://tenant-a/content-a",
               storage_mode: :content_addressed,
               schema_ref: "schema://result",
               redaction_ref: "redaction://standard",
               content_ref: "content://tenant-a/hash-a",
               content_hash:
                 "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9",
               byte_size: 12,
               store_ref: "store://local"
             })

    assert result.storage_mode == :content_addressed
  end

  test "content store put/fetch/retention is pure and fail closed" do
    assert {:ok, store, entry} =
             ContentStore.put(%{}, %{
               content_ref: "content://tenant-a/hash-a",
               owner_ref: "operation-context://tenant-a/request-a",
               tenant_ref: "tenant://tenant-a",
               installation_ref: "installation://tenant-a/product-a/install-a",
               schema_ref: "schema://payload",
               redaction_ref: "redaction://standard",
               content_hash:
                 "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9",
               byte_size: 12,
               body: "hello world!",
               retention_refs: ["receipt://tenant-a/receipt-a"]
             })

    assert entry.content_ref == "content://tenant-a/hash-a"

    assert {:ok, _entry} =
             ContentStore.fetch(store, "content://tenant-a/hash-a", %{
               tenant_ref: "tenant://tenant-a",
               installation_ref: "installation://tenant-a/product-a/install-a"
             })

    assert {:error, :unauthorized_content_access} =
             ContentStore.fetch(store, "content://tenant-a/hash-a", %{
               tenant_ref: "tenant://other",
               installation_ref: "installation://tenant-a/product-a/install-a"
             })

    assert {:error, {:content_retained, ["receipt://tenant-a/receipt-a"]}} =
             ContentStore.delete(store, "content://tenant-a/hash-a")
  end
end
