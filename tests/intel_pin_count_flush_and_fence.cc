#include <iostream>
#include <fstream>
#include <vector>
#include "pin.H"

using std::cerr;
using std::ofstream;
using std::ios;
using std::string;
using std::endl;

ofstream OutFile;

// The running count of instructions is kept here
// make it static to help the compiler optimize docount
bool flag = false;
uint64_t CLWB_cnt = 0;
uint64_t CLFLUSH_cnt = 0;
uint64_t CLFLUSHOPT_cnt = 0;
uint64_t SFENCE_cnt = 0;
uint64_t MFENCE_cnt = 0;
uint64_t WBINVD_cnt = 0;

VOID CLWB_count()       { if (flag) CLWB_cnt++; }
VOID CLFLUSH_count()    { if (flag) CLFLUSH_cnt++; }
VOID CLFLUSHOPT_count() { if (flag) CLFLUSHOPT_cnt++; }
VOID SFENCE_count()     { if (flag) SFENCE_cnt++; }
VOID MFENCE_count()     { if (flag) MFENCE_cnt++; }
VOID WBINVD_count()     { if (flag) WBINVD_cnt++; }

VOID begin() { flag = true; }
VOID end() { flag = false; }

// Pin calls this function every time a new instruction is encountered
VOID Instruction(INS ins, VOID *v)
{
    string op = INS_Mnemonic(ins);
    // Insert a call to docount before every instruction, no arguments are passed
    if (op == "CLWB")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)CLWB_count, IARG_END);
    if (op == "CLFLUSH")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)CLFLUSH_count, IARG_END);
    if (op == "CLFLUSHOPT")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)CLFLUSHOPT_count, IARG_END);
    if (op == "SFENCE")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)SFENCE_count, IARG_END);
    if (op == "MFENCE")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)MFENCE_count, IARG_END);
    if (op == "WBINVD")
        INS_InsertCall(ins, IPOINT_BEFORE, (AFUNPTR)WBINVD_count, IARG_END);
}

// Pin calls this function every time a new rtn is executed
VOID Routine(RTN rtn, VOID *v)
{
    string name = RTN_Name(rtn);

    RTN_Open(rtn);

    if (name.find("intel_pin_start") != string::npos) {
        // Insert a call at the entry point of a routine to increment the call count
        RTN_InsertCall(rtn, IPOINT_AFTER, (AFUNPTR)begin, IARG_END);
    }

    if (name.find("intel_pin_stop") != string::npos) {
        // Insert a call at the entry point of a routine to increment the call count
        RTN_InsertCall(rtn, IPOINT_BEFORE, (AFUNPTR)end, IARG_END);
    }

    RTN_Close(rtn);
}

KNOB<string> KnobOutputFile(KNOB_MODE_WRITEONCE, "pintool",
    "o", "inscount.out", "specify output file name");

// This function is called when the application exits
VOID Fini(INT32 code, VOID *v)
{
    // Write to a file since cout and cerr maybe closed by the application
    OutFile.setf(ios::showbase);
    OutFile << "CLWB: " << CLWB_cnt << endl;
    OutFile << "CLFLUSH: " << CLFLUSH_cnt << endl;
    OutFile << "CLFLUSHOPT: " << CLFLUSHOPT_cnt << endl;
    OutFile << "MFENCE: " << MFENCE_cnt << endl;
    OutFile << "SFENCE: " << SFENCE_cnt << endl;
    OutFile << "WBINVD: " << WBINVD_cnt << endl;
    OutFile << endl;
    OutFile << "Total: " << CLWB_cnt + CLFLUSH_cnt + CLFLUSHOPT_cnt + MFENCE_cnt + SFENCE_cnt + WBINVD_cnt << endl;
    OutFile.close();
}

/* ===================================================================== */
/* Print Help Message                                                    */
/* ===================================================================== */

INT32 Usage()
{
    cerr << "This tool counts the number of dynamic instructions executed" << endl;
    cerr << endl << KNOB_BASE::StringKnobSummary() << endl;
    return -1;
}

/* ===================================================================== */
/* Main                                                                  */
/* ===================================================================== */
/*   argc, argv are the entire command line: pin -t <toolname> -- ...    */
/* ===================================================================== */

int main(int argc, char * argv[])
{
    // Initialize symbol table code, needed for rtn instrumentation
    PIN_InitSymbols();

    // Initialize pin
    if (PIN_Init(argc, argv)) return Usage();

    OutFile.open(KnobOutputFile.Value().c_str());

    // Register Instruction to be called to instrument instructions
    INS_AddInstrumentFunction(Instruction, 0);

    // Register Routine to be called to instrument rtn
    RTN_AddInstrumentFunction(Routine, 0);

    // Register Fini to be called when the application exits
    PIN_AddFiniFunction(Fini, 0);

    // Start the program, never returns
    PIN_StartProgram();

    return 0;
}
