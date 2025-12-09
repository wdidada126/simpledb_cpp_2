#include "simpledb/server/simpledb.hpp"
#include "simpledb/tx/transaction.hpp"
#include "simpledb/record/schema.hpp"
#include "simpledb/record/layout.hpp"
#include "simpledb/record/record_page.hpp"

int main() {
    simpledb::server::SimpleDB db("testdb", 4096, 10);
    auto tx = db.new_tx();
    simpledb::record::Schema sch{};
    sch.add_int_field("a");
    sch.add_string_field("B", 9);
    simpledb::record::Layout layout{sch};
    for (const auto& field : sch.fields()) {
        int offset = layout.offset(field);
        std::cout << "offset of " << field << " is " << offset << std::endl;
    }
    auto blk = tx->append("testfile");
    tx->pin(blk);
    simpledb::record::RecordPage rp{tx, blk, layout};
    rp.format();

    std::cout << "Filling the page with random records..." << std::endl;
    int slot = rp.insert_after(-1);
    while (rp.is_slot_valid(slot)) {
        if (slot < 0) {
            throw std::runtime_error("Slot is negative");
        }
        std::cout << "Inserting record at slot " << slot << std::endl;
        int n = (int)(50 * rand());
        rp.set_int(slot, "A", n);
        //rp.set_string(slot, "B", "Hello");
        std::cout << "Inserted record " << n << " at slot " << slot << std::endl;
        slot = rp.insert_after(slot);
    }

    std::cout << "Will Delete records with a < 25" << std::endl;
    slot = 0;
    int count = 0;
    while (rp.is_slot_valid(slot)) {
        int a = rp.get_int(slot, "A");
        auto b = rp.get_string(slot, "B");
        if (a < 25) {
            ++count;
            std::cout << "Deleting record " << a << " " << b << " at slot " << slot << std::endl;
            rp.delete_slot(slot);
        }
        slot = rp.next_after(slot);
    }
    std::cout << "Deleted " << count << " records" << std::endl;
    std::cout << "Will print the page" << std::endl;
    slot = rp.next_after(-1);
    while (rp.is_slot_valid(slot)) {
        int a = rp.get_int(slot, "A");
        auto b = rp.get_string(slot, "B");
        std::cout << "Record: " << a << " " << b << " at slot " << slot << std::endl;
        slot = rp.next_after(slot);
    }
    tx->unpin(blk);
    tx->commit();

}