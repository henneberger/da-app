import { NextResponse } from "next/server";
import { getNamespacesLabeledAsProjects } from "@/lib/k8s";
import { errorMessage } from "@/lib/errors";

export const runtime = "nodejs";

export async function GET() {
  try {
    const projects = await getNamespacesLabeledAsProjects();
    return NextResponse.json({ projects });
  } catch (e: unknown) {
    return NextResponse.json(
      { error: errorMessage(e) },
      { status: 500 },
    );
  }
}
