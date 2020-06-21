#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include <cmath>
#include <algorithm>

#define READ 1
#define WRITE 2
#define ROOT_OFFSET_MOD (VIRTUAL_ADDRESS_WIDTH % OFFSET_WIDTH)
#define ROOT_OFFSET_WIDTH ((ROOT_OFFSET_MOD) ? ROOT_OFFSET_MOD : OFFSET_WIDTH)
#define VIRTUAL_ADDRESS_COUNT (1 << VIRTUAL_ADDRESS_WIDTH)
#define FAILURE 0
#define SUCCESS 1

void clearTable(uint64_t frameIndex)
{
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize()
{
    for (word_t i = 0; i < NUM_FRAMES; i++)
    {
        clearTable(i);
    }
}

struct dfs_DATA
{
    word_t previousAddress;
    int maxCyclicDistance;
    uint64_t farthestVirtualAddressByCyclic;
    word_t farthestFrameIdByCyclic;
    word_t farthestCyclicParent; // The parent of the frame we evict. Used to delete pointer to evicted frame.
    uint64_t farthestCyclicOffsetInParent; // the offset of the frame we evict in its parent. used to delete pointer to evicted frame
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

void DFS(dfs_DATA *my_data, unsigned int currentLayer, uint64_t pageID, word_t addr, word_t parentAddr)
{
    if (my_data->freeFrameFound)
    {
        return;
    }

    if (my_data->maxFrameUsed < addr)
    {
        my_data->maxFrameUsed = addr;
    }

    if (currentLayer == TABLES_DEPTH)
    {
        int cyclicDistance = calcCyclicDistance(pageID, my_data->destinationPageID);
        if (cyclicDistance > my_data->maxCyclicDistance)
        {
            my_data->maxCyclicDistance = cyclicDistance;
            my_data->farthestVirtualAddressByCyclic = pageID;
            my_data->farthestFrameIdByCyclic = addr;
            my_data->farthestCyclicParent = parentAddr;
            //the pageId now is the pageId without the offset in it. The current layer is the last layer.
            my_data->farthestCyclicOffsetInParent = pageID & ((1LL << OFFSET_WIDTH) - 1);
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
            .farthestVirtualAddressByCyclic = 0, .farthestFrameIdByCyclic=0, .farthestCyclicParent=0, .farthestCyclicOffsetInParent=0,
            .maxFrameUsed = 0, .freeFrameFound = false,
            .freeFrameResult = 0, .destinationPageID = destinationPageID};
    unsigned int currentLayer = 0;
    DFS(&dfs_data, currentLayer, 0, 0, 0);

    if (dfs_data.freeFrameFound)
    {
//        after we found the free frame we erased the pointer to it from its parent.
        return dfs_data.freeFrameResult;
    }

    if (dfs_data.maxFrameUsed != NUM_FRAMES - 1)
    {
        return 1 + dfs_data.maxFrameUsed;
    }


    PMevict((uint64_t) dfs_data.farthestFrameIdByCyclic, dfs_data.farthestVirtualAddressByCyclic);
    // delete the pointer to the evicted frame from its parent
    PMwrite(dfs_data.farthestCyclicParent * PAGE_SIZE + dfs_data.farthestCyclicOffsetInParent, 0);
    // clear the page we evict. to avoid reading values we inserted in the past as valid pointers.
    clearTable(dfs_data.farthestFrameIdByCyclic);
    return dfs_data.farthestFrameIdByCyclic;

}

int operationWrapper(uint64_t virtualAddress, word_t *value, int operation)
{
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return FAILURE;
    }
    uint64_t pageID = virtualAddress >> OFFSET_WIDTH;
    word_t addr = 0;
    word_t pmValue;
    unsigned int currentLayer = 0;
    word_t freeFrame;

    while (currentLayer < TABLES_DEPTH)
    {
        uint64_t currentOffset = getCurrentOffset(currentLayer, virtualAddress);
        PMread(addr * PAGE_SIZE + currentOffset, &pmValue);
        if (pmValue == 0)
        {
            freeFrame = findFreeFrame(addr, pageID);

            if (currentLayer < (unsigned int)(TABLES_DEPTH - 1))
            {
                clearTable(freeFrame);
            } else
            {
                PMrestore(freeFrame, pageID);
            }
            pmValue = freeFrame;
            PMwrite(addr * PAGE_SIZE + currentOffset, pmValue);
        }
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
    return SUCCESS;
}


int VMread(uint64_t virtualAddress, word_t *value)
{
    return operationWrapper(virtualAddress, value, READ);
}

int VMwrite(uint64_t virtualAddress, word_t value)
{
    return operationWrapper(virtualAddress, &value, WRITE);
}
