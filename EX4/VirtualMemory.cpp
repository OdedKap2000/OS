#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include <cmath>
#include <algorithm>

#define READ 1
#define WRITE 2
#define ROOT_OFFSET_WIDTH (VIRTUAL_ADDRESS_WIDTH % OFFSET_WIDTH)
#define VIRTUAL_ADDRESS_COUNT (1 << VIRTUAL_ADDRESS_WIDTH)

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

struct dfs_DATA
{
    word_t previousAddress;
    int maxCyclicDistance;
    uint64_t farthestVirtualAddressByCyclic;
    word_t farthestFrameIdByCyclic;
    word_t maxFrameUsed;
    bool freeFrameFound;
    word_t freeFrameResult;
    uint64_t destinationPageID;
};

int min(int a, int b)
{
    if (a < b)
        return a;
    return b;
}

int calcCyclicDistance(uint64_t a, uint64_t b)
{
    int intA = (int) a;
    int intB = (int) b;
    int dist = std::abs(intA - intB);
    return min(dist, VIRTUAL_ADDRESS_COUNT - dist);
}

void DFS(dfs_DATA *my_data, int currentLayer, uint64_t pageID, word_t addr, word_t parentAddr)
{
    if (my_data->freeFrameFound)
    {
        return;
    }

    if (currentLayer == TABLES_DEPTH)
    {
        int cyclicDistance = calcCyclicDistance(pageID, my_data->destinationPageID);
        if (cyclicDistance > my_data->maxCyclicDistance)
        {
            my_data->maxCyclicDistance = cyclicDistance;
            my_data->farthestVirtualAddressByCyclic = pageID;
            my_data->farthestFrameIdByCyclic = addr;
        }
        return;
    }

    long childrenAmount;

    if (currentLayer == 0)
    {
        childrenAmount = 1 << ROOT_OFFSET_WIDTH;
    } else
    {
        childrenAmount = 1 << OFFSET_WIDTH;
    }

    if (my_data->maxFrameUsed < addr)
    {
        my_data->maxFrameUsed = addr;
    }

    bool allZeros = true;
    word_t pmValue;

    for (int i = 0; i < childrenAmount; ++i)
    {
        PMread((uint64_t) (addr * PAGE_SIZE + i), &pmValue);
        if (pmValue != 0)
        {
            allZeros = false;
            DFS(my_data, currentLayer + 1, (pageID << OFFSET_WIDTH) + i, pmValue, addr);
        }
    }

    if (allZeros && (addr != my_data->previousAddress))
    {
        my_data->freeFrameFound = true;
        my_data->freeFrameResult = addr;
        uint64_t myOffset = pageID & ((1 << OFFSET_WIDTH) - 1);
        PMwrite(parentAddr * PAGE_SIZE + myOffset, 0);
    }

}

word_t findFreeFrame(word_t previousAddress, uint64_t destinationPageID)
{
    // Find an unused frame or evict a page from some frame.
    // Make sure it's not the previous address
    dfs_DATA dfs_data = dfs_DATA{.previousAddress=previousAddress, .maxCyclicDistance =  0,
            .farthestVirtualAddressByCyclic = 0, .farthestFrameIdByCyclic=0,
            .maxFrameUsed = 0, .freeFrameFound = false,
            .freeFrameResult = 0, .destinationPageID = destinationPageID};
    DFS(&dfs_data, 0, 0, 0, 0);

    if (dfs_data.freeFrameFound)
    {
//        after we found the free frame we erased the pointer to it from its parent.
        return dfs_data.freeFrameFound;
    }

    if (dfs_data.maxFrameUsed != NUM_FRAMES - 1)
    {
        return 1 + dfs_data.maxFrameUsed;
    }

    PMevict((uint64_t) dfs_data.farthestFrameIdByCyclic, dfs_data.farthestVirtualAddressByCyclic);
    return dfs_data.farthestFrameIdByCyclic;

}

uint64_t getCurrentOffset(unsigned int currentLayer, uint64_t virtualAddress)
{
    if (currentLayer == 0)
    {
        uint64_t mask = ((1LL << ROOT_OFFSET_WIDTH) - 1) << (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));
        uint64_t maskedOffset = virtualAddress & mask;
        return maskedOffset >> (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));
    }
    uint64_t mask = ((1LL << OFFSET_WIDTH) - 1) << (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));
    uint64_t maskedOffset = virtualAddress & mask;
    return maskedOffset >> (OFFSET_WIDTH * (TABLES_DEPTH - currentLayer));
}

int operationWrapper(uint64_t virtualAddress, word_t *value, int operation)
{
    uint64_t pageID = virtualAddress >> OFFSET_WIDTH;
    word_t addr = 0;
    word_t pmValue;
    unsigned int currentLayer = 0;
    word_t previousAddress = 0;
    word_t freeFrame;

    while (currentLayer < TABLES_DEPTH)
    {
        uint64_t currentOffset = getCurrentOffset(currentLayer, virtualAddress);
        PMread(addr * PAGE_SIZE + currentOffset, &pmValue);
        if (pmValue == 0)
        {
            freeFrame = findFreeFrame(previousAddress, pageID);

            if (currentLayer < TABLES_DEPTH - 1)
            {
                clearTable(freeFrame);
            } else
            {
                PMrestore(freeFrame, pageID);
            }
            pmValue = freeFrame;
            PMwrite(addr * PAGE_SIZE + currentOffset, pmValue);
        }
        previousAddress = addr;
        addr = pmValue;
        currentLayer++;
    }
    if (operation == READ)
    {
        PMread(addr * PAGE_SIZE + getCurrentOffset(TABLES_DEPTH, virtualAddress), value);
    } else if (operation == WRITE)
    {
        PMwrite(addr * PAGE_SIZE + getCurrentOffset(TABLES_DEPTH, virtualAddress), *value);
    }
    return 1;
}


int VMread(uint64_t virtualAddress, word_t *value)
{
    return operationWrapper(virtualAddress, value, READ);
}

int VMwrite(uint64_t virtualAddress, word_t value)
{
    return operationWrapper(virtualAddress, &value, WRITE);
}
