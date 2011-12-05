#include <memory>

class S8Solver;
std::auto_ptr<S8Solver> s;
// extraOpt = 0 -- NORMAL
// extraOpt = 1 -- mcTrue
// extraOpt = 2 -- ptrel12
// extraOpt = 3 -- ptrel05

void run_s8( const char * data, const char * mc, int extraOpt = 0, TArrayD binList) {

    gSystem->Load("libS8Solver.so");

    s.reset(new S8Solver());
    s->SetData( data );
    s->SetCorrData( mc );
    s->SetPtrelMaxCut(-1);
    s->SetAlphaConstant(false);
    s->SetBetaConstant(false);
    s->SetGammaConstant(false);
    s->SetDeltaConstant(false);
    s->SetKappabConstant(false);
    s->SetKappaclConstant(false);
    s->setDoAverageSolution(true);

    //s->SetEtaFits();
//    s->setFirstBin(1);
//    s->setLastBin(6);
    //s->SetSolution(0, 1);
    //s->SetSolution(1, 1);
    //s->SetSolution(2, 1);
    //s->SetSolution(3, 1);
    //s->SetSolution(4, 1);
    //s->SetSolution(5, 1);
    //s->SetSolution(6, 1);
    //s->SetSolution(7, 1);
    //s->SetSolution(8, 1);
    //s->SetSolution(9, 1);
    //s->SetSolution(10, 1);
    if ( (extraOpt == 1) || (extraOpt == 3) ) {
        s->UseMCTrue(false);
    } else {
        s->UseMCTrue(true);
    }
    if ( extraOpt == 2 ) {
        s->SetPtrelCut( 1.2 );
    }
    if ( extraOpt == 3 ) {
        s->SetPtrelCut( 0.5 );
    }
//    if (binCount != 0) {
        Double_t xbins[4] = { 20,50,80,240};
        s->SetRebin(4, xbins);
//    }
//    Double_t xbins[10] = {20,30,40,50,60,70,80,100,120,240};
//      s->SetRebin(10, xbins);
//    s->SetPseudoExperiments( 1000 );
    s->Solve();
    s->PrintData();
    s->Draw();
    s->Save("s8.root");
}
