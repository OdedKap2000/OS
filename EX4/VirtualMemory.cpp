#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include <cmath>


void clearTable(uint64_t frameIndex)
{
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize()
{
    clearTable(0);
}

uint64_t findFreeFrame(uint64_t previousAddress)
{
    // Find an unused frame or evict a page from some frame.
    // Make sure it's not the previous addresss

}

uint64_t getCurrentOffset(unsigned int currentLayer, uint64_t virtualAddress)
{
//    std::pow(2, OFFSET_WIDTH) - 1) is 1111..11 in length of OFFSET_WIDTH
//    std::pow(2, (TABLES_DEPTH - currentLayer)) is the multiplier of the mask so it would match the current offset
    uint64_t mask = ((1LL << OFFSET_WIDTH) - 1) << (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));
    uint64_t maskedOffset = virtualAddress & mask;
    return maskedOffset >> (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));

}

int VMread(uint64_t virtualAddress, word_t *value)
{
    uint64_t pageID = virtualAddress >> OFFSET_WIDTH;
    uint64_t addr = 0;
    word_t pmValue;
    unsigned int currentLayer = 0;
    word_t previousAddress = 0;
    uint64_t freeFrame;
    while (currentLayer < TABLES_DEPTH)
    {
        uint64_t currentOffset = getCurrentOffset(currentLayer, virtualAddress);
        PMread(addr * PAGE_SIZE + currentOffset, &pmValue);
        if (pmValue == 0)
        {
            freeFrame = findFreeFrame(previousAddress);

            if (currentLayer < TABLES_DEPTH - 1)
            {
                clearTable(freeFrame);
            } else
            {
                PMrestore(freeFrame, pageID);
            }
        }
        previousAddress = pmValue;
        currentLayer++;
    }
    PMread(addr*PAGE_SIZE +)
    // If its a write command: PMwrite(addr * PAGE_SIZE + lastOffset, value)
    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value)
{
    return 1;
}
